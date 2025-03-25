package dst_table

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	client "aardappel/internal/util/ydb"
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"go.uber.org/zap"
)

type PushQuery struct {
	Query              string
	Parameters         table.QueryParameters
	ModificationsCount int
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
	client    *client.TableClient
	tablePath string
	tableInfo TableMetaInfo
	MonTag    string
}

func (d *DstTable) GetTablePath() string {
	return d.tablePath
}

func NewDstTable(client *client.TableClient, tablePath string, monTag string) *DstTable {
	var dstTable DstTable
	dstTable.client = client
	dstTable.tablePath = tablePath
	dstTable.MonTag = monTag
	return &dstTable
}

func DescribeTable(ctx context.Context, client *client.TableClient, tablePath string) (*options.Description, error) {
	var desc options.Description
	err := client.Do(ctx, func(ctx context.Context, s table.Session) error {
		var err error
		desc, err = s.DescribeTable(ctx, tablePath)
		return err
	}, table.WithIdempotent())
	if err != nil {
		xlog.Error(ctx, "Unable to describe table", zap.Error(err))
		return nil, fmt.Errorf("DescribeTable: %w", err)
	}
	return &desc, nil
}

func (dstTable *DstTable) describeTable(ctx context.Context) (*options.Description, error) {
	return DescribeTable(ctx, dstTable.client, dstTable.tablePath)
}

func (dstTable *DstTable) Init(ctx context.Context) error {
	desc, err := dstTable.describeTable(ctx)
	if err != nil {
		xlog.Error(ctx, "Unable to init dst table", zap.Error(err), zap.String("path", dstTable.tablePath))
		return fmt.Errorf("Init: %w", err)
	}
	metaInfo := NewTableMetaInfo()
	metaInfo.PrimaryKey = desc.PrimaryKey
	metaInfo.Name = dstTable.tablePath
	for _, column := range desc.Columns {
		metaInfo.Columns[column.Name] = column
		xlog.Debug(ctx, "Column type", zap.String("col_name", column.Name), zap.String("type", column.Type.String()))
	}
	xlog.Debug(ctx, "Got table meta info", zap.Strings("primary_keys", metaInfo.PrimaryKey), zap.Any("columns", metaInfo.Columns))
	dstTable.tableInfo = *metaInfo
	return nil
}

func (dstTable *DstTable) GenQuery(ctx context.Context, txData []types.TxData, globalStatementNum int) (PushQuery, error) {
	query, err := GenQuery(ctx, dstTable.tableInfo, txData, globalStatementNum)
	if err != nil {
		xlog.Error(ctx, "Can't gen query", zap.Error(err))
	}
	return query, err
}
