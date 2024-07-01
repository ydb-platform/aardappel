package pusher

import (
	"aardappel/internal/dst_table"
	"aardappel/internal/types"
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func PushAsSingleTx(ctx context.Context, client table.Client, data dst_table.PushQuery, position types.Position, stateTable string) error {
	stateStoreQuery := fmt.Sprintf(`
--DECLARE $stateStepId AS Uint64;
--DECLARE $stateTxId AS Uint64;

UPSERT INTO
    %v
    (id, step_id, tx_id)
VALUES
    (0, $stateStepId, $stateTxId);
`, stateTable)
	stateParam := table.NewQueryParameters(
		table.ValueParam("$stateStepId", ydbTypes.Uint64Value(position.Step)),
		table.ValueParam("$stateTxId", ydbTypes.Uint64Value(position.TxId)),
	)
	param := append(data.Parameters, *stateParam...)

	return client.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			_, err := tx.Execute(ctx, data.Query+stateStoreQuery, &param)
			return err
		})
}
