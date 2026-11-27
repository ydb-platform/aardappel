package processor_test

import (
	"aardappel/internal/processor"
	"aardappel/tests/common/local_ydb"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

func TestSchemaRefresh(t *testing.T) {
	localYDB := local_ydb.StartupYdb(t, *local_ydb.NewYdbSettings())
	env := newReplicationTestEnv(t, &localYDB, false, processor.STAGE_RUN, 1)
	stream := env.streams[0]

	run := env.startProcessor()
	defer run.stop()
	env.executeScheme(fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN new_value Uint64", stream.dstTable))
	local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0,
		`{"update":{"new_value":42},"key":[1],"ts":[1,1]}`,
		`{"resolved":[2,0]}`,
	)

	_, err := run.processor.DoReplication(run.ctx, run.dstTables, run.executeTransaction)
	require.NoError(t, err)

	var newValue uint64
	err = env.client.TableClient.DoTx(env.ctx, func(ctx context.Context, tx table.TransactionActor) error {
		result, err := tx.Execute(ctx, fmt.Sprintf("SELECT new_value FROM `%s` WHERE id = 1", stream.dstTable), nil)
		if err != nil {
			return err
		}
		if !result.NextResultSet(ctx) || !result.NextRow() {
			return fmt.Errorf("replicated row not found")
		}
		return result.ScanNamed(named.OptionalWithDefault("new_value", &newValue))
	})
	require.NoError(t, err)
	require.Equal(t, uint64(42), newValue)
}
