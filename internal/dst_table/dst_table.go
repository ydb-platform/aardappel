package dst_table

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"go.uber.org/zap"
)

type PushQuery struct {
	Query      string
	Parameters table.QueryParameters
}

type TableMetaInfo struct {
	PrimaryKey []string
	Columns    map[string]options.Column
	Name       string
}

func NewTableMetaInfo() *TableMetaInfo {
	return &TableMetaInfo{PrimaryKey: make([]string, 0), Columns: make(map[string]options.Column)}
}

type DstTable struct {
	client    table.Client
	tablePath string
	tableInfo TableMetaInfo
}

func NewDstTable(client table.Client, tablePath string) *DstTable {
	var dstTable DstTable
	dstTable.client = client
	dstTable.tablePath = tablePath
	return &dstTable
}

func (dstTable *DstTable) DescribeTable(ctx context.Context) (*options.Description, error) {
	var desc options.Description
	err := dstTable.client.Do(ctx, func(ctx context.Context, s table.Session) error {
		var err error
		desc, err = s.DescribeTable(ctx, dstTable.tablePath)
		return err
	}, table.WithIdempotent())
	if err != nil {
		xlog.Error(ctx, "Unable to describe table", zap.Error(err))
		return nil, fmt.Errorf("DescribeTable: %w", err)
	}
	return &desc, nil
}

func (dstTable *DstTable) Init(ctx context.Context) error {
	desc, err := dstTable.DescribeTable(ctx)
	if err != nil {
		xlog.Error(ctx, "Unable to init dst table", zap.Error(err), zap.String("path", dstTable.tablePath))
		return fmt.Errorf("Init: %w", err)
	}
	metaInfo := NewTableMetaInfo()
	metaInfo.PrimaryKey = desc.PrimaryKey
	metaInfo.Name = desc.Name
	for _, column := range desc.Columns {
		metaInfo.Columns[column.Name] = column
		xlog.Debug(ctx, "Column type", zap.String("col_name", column.Name), zap.String("type", column.Type.String()))
	}
	xlog.Debug(ctx, "Got table meta info", zap.Strings("primary_keys", metaInfo.PrimaryKey), zap.Any("columns", metaInfo.Columns))
	dstTable.tableInfo = *metaInfo
	return nil
}

func (dstTable *DstTable) PushAsSingleTx(ctx context.Context, data PushQuery) error {
	return dstTable.client.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			_, err := tx.Execute(ctx, data.Query, &data.Parameters)
			return err
		})
}

func (dstTable *DstTable) Push(ctx context.Context, txData []types.TxData) error {
	query, err := GenQuery(ctx, dstTable.tableInfo, txData)
	if err != nil {
		xlog.Error(ctx, "Can't gen query", zap.Error(err))
		return fmt.Errorf("Push: %w", err)
	}
	err = dstTable.PushAsSingleTx(ctx, query)
	if err != nil {
		xlog.Error(ctx, "Can't push query", zap.Error(err))
		return fmt.Errorf("Push: %w", err)
	}
	return nil
}
