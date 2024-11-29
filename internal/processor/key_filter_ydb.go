package processor

import (
	"aardappel/internal/util/xlog"
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"sync"
)

type YdbMemoryKeyFilter struct {
	path            string
	keys            sync.Map
	dstServerClient table.Client
	size            uint64
	instanceId      string
}

const storeBatchSz = 100

func storeKeys(ctx context.Context, path string, instanceId string, keys [][]byte, dstClient table.Client) {
	q := fmt.Sprintf("UPSERT INTO `%v` SELECT * FROM AS_TABLE ($keys);", path)

	values := make([]ydb_types.Value, 0, len(keys))
	xlog.Debug(ctx, "store keys", zap.Int("total in batch", len(keys)))

	for _, key := range keys {
		values = append(values, ydb_types.StructValue(
			ydb_types.StructFieldValue("instance_id", ydb_types.UTF8Value(instanceId)),
			ydb_types.StructFieldValue("key", ydb_types.BytesValue(key))))
	}

	param := []table.ParameterOption{table.ValueParam("$keys", ydb_types.ListValue(values...))}

	err := dstClient.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			_, err := tx.Execute(ctx, q, table.NewQueryParameters(param...))
			return err
		})

	if err != nil {
		xlog.Fatal(ctx, "Unable to store keys filter", zap.Error(err))
	}
}

func readKeys(ctx context.Context, client table.Client, path string, instanceId string, feedKeys func(batch [][]byte)) error {
	return client.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			res, err := s.StreamReadTable(ctx, path,
				options.ReadColumn("key"),
				options.ReadGreaterOrEqual(ydb_types.TupleValue(
					ydb_types.OptionalValue(ydb_types.UTF8Value(instanceId)))),
				options.ReadLessOrEqual(ydb_types.TupleValue(
					ydb_types.OptionalValue(ydb_types.UTF8Value(instanceId)))))
			if err != nil {
				xlog.Error(ctx, "unable to start key filter table read", zap.Error(err))
				return err
			}
			defer func() {
				_ = res.Close()
			}()

			for res.NextResultSet(ctx) {
				var batch [][]byte
				for res.NextRow() {
					var key *[]byte
					err = res.ScanNamed(
						named.Optional("key", &key),
					)
					if err != nil {
						xlog.Error(ctx, "unable to get key value", zap.Error(err))
						return err
					}
					batch = append(batch, *key)
				}
				feedKeys(batch)
				xlog.Debug(ctx, "key filter batch loaded", zap.Int("num keys", len(batch)))
			}
			return nil
		})
}

func NewYdbMemoryKeyFilter(ctx context.Context, client *table.Client, filterTablePath string, instanceId string) (*YdbMemoryKeyFilter, error) {
	var filter YdbMemoryKeyFilter

	if len(filterTablePath) != 0 && client != nil {
		xlog.Debug(ctx, "try to read key filter table", zap.String("path", filterTablePath))
		err := readKeys(ctx, *client, filterTablePath, instanceId, func(batch [][]byte) {
			for _, key := range batch {
				filter.keys.Store(string(key), struct{}{})
			}
		})
		if err != nil {
			return nil, err
		}
		filter.path = filterTablePath
		filter.dstServerClient = *client
		filter.instanceId = instanceId
	}

	return &filter, nil
}

func (f *YdbMemoryKeyFilter) AddKeysToBlock(ctx context.Context, keys [][]byte) {
	if len(f.path) > 0 {
		var pos int
		xlog.Debug(ctx, "AddKeys", zap.Int("sz:", len(keys)))
		for i, _ := range keys {
			xlog.Debug(ctx, "xx", zap.Int("pos", pos), zap.Int("i", i))
			if i-pos > storeBatchSz {
				storeKeys(ctx, f.path, f.instanceId, keys[pos:i], f.dstServerClient)
				pos = i
			}
		}

		if len(keys[pos:]) > 0 {
			storeKeys(ctx, f.path, f.instanceId, keys[pos:], f.dstServerClient)
		}
	}

	for _, key := range keys {
		//z[*((*string)(unsafe.Pointer(&y)))] = struct{}{}
		f.keys.Store(string(key), struct{}{})
	}
}

func (f *YdbMemoryKeyFilter) Filter(ctx context.Context, key []byte) bool {
	_, filtered := f.keys.Load(string(key))
	return filtered
}

func (f *YdbMemoryKeyFilter) GetBlockedKeysCount() uint64 {
	return f.size
}
