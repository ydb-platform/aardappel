package ydb_operations

import (
	"aardappel/internal/util/xlog"
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

func SomeYdbOperation(ctx context.Context, client table.Client) error {
	return client.Do(ctx, func(ctx context.Context, s table.Session) error {
		query := `
			DECLARE $x AS Uint64;
			SELECT $x as y;
		`
		expr, err := s.Explain(ctx, query)
		if err != nil {
			return fmt.Errorf("explain error: %w", err)
		}
		fmt.Println(expr.AST)

		readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly(table.WithInconsistentReads())), table.CommitTx())
		_, res, err := s.Execute(ctx, readTx, query,
			table.NewQueryParameters(
				table.ValueParam("$x", types.Uint64Value(12345)),
			),
			options.WithCollectStatsModeBasic(),
		)

		if err != nil {
			return fmt.Errorf("execute error: %w", err)
		}
		defer res.Close()

		if err = res.NextResultSetErr(ctx); err != nil {
			return fmt.Errorf("next result set error %d: %w", err)
		}

		if !res.NextRow() {
			return fmt.Errorf("no rows in result: %w", err)
		}
		var y uint64
		err = res.ScanNamed(named.OptionalWithDefault("y", &y))

		xlog.Info(ctx, "read succeed", zap.Int("y", int(y)))

		return res.Err()
	})
}
