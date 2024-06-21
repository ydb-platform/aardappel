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
	Query string
}

type TableMetaInfo struct {
	PrimaryKey []string
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
		return nil, fmt.Errorf("describe table: %w", err)
	}
	return &desc, nil
}

func (dstTable *DstTable) Init(ctx context.Context) error {
	desc, err := dstTable.DescribeTable(ctx)
	if err != nil {
		xlog.Error(ctx, "Unable to init dst table", zap.Error(err), zap.String("path", dstTable.tablePath))
		return fmt.Errorf("init: %w", err)
	}
	var metaInfo TableMetaInfo
	metaInfo.PrimaryKey = desc.PrimaryKey
	xlog.Debug(ctx, "Got table meta info", zap.Strings("primary_keys", metaInfo.PrimaryKey))
	dstTable.tableInfo = metaInfo
	return nil
}

func (dstTable *DstTable) GenQuery(ctx context.Context, txData types.TxData) (PushQuery, error) {
	return PushQuery{}, nil
}

func (dstTable *DstTable) Push(ctx context.Context, txData types.TxData) error {
	_, _ = dstTable.GenQuery(ctx, txData)
	return nil
}
