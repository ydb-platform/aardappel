package dst_table

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

type QueryStatement struct {
	Statement string
	Params    []table.ParameterOption
}

func NewQueryStatement() *QueryStatement {
	return &QueryStatement{
		Statement: "",
		Params:    make([]table.ParameterOption, 0),
	}
}

func StringToUUID(s string) ([16]byte, error) {
	var byteArray [16]byte
	byteSlice := []byte(s)
	if len(byteSlice) > 16 {
		return byteArray, fmt.Errorf("StringToUUID: string is too long to fit in [16]byte")
	}
	copy(byteArray[:], byteSlice)
	return byteArray, nil
}

func ConvertToYDBValue(v json.RawMessage, t ydb_types.Type) (ydb_types.Value, error) {
	var err error
	if isOptional, innerType := ydb_types.IsOptional(t); isOptional {
		var nulValue interface{}
		if err = json.Unmarshal(v, &nulValue); err == nil {
			if nulValue == nil {
				return ydb_types.NullValue(innerType), nil
			}
		}
		if err != nil {
			return nil, fmt.Errorf("ConvertToYDBValue: error unmurshal value: %w", err)
		}

		v, err := ConvertToYDBValue(v, innerType)
		if err != nil {
			if err != nil {
				return nil, fmt.Errorf("ConvertToYDBValue: error get optional value with inner type: %w", err)
			}
			return nil, err
		}
		return ydb_types.OptionalValue(v), nil
	}

	switch t {
	case ydb_types.TypeBool:
		var value bool
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.BoolValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to bool value: %w", err)
	case ydb_types.TypeInt8:
		var value int8
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Int8Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to int8 value: %w", err)
	case ydb_types.TypeInt16:
		var value int16
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Int16Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to int16 value: %w", err)
	case ydb_types.TypeInt32:
		var value int32
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Int32Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to int32 value: %w", err)
	case ydb_types.TypeInt64:
		var value int64
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Int64Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to int64 value: %w", err)
	case ydb_types.TypeUint8:
		var value uint8
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Uint8Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uint8 value: %w", err)
	case ydb_types.TypeUint16:
		var value uint16
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Uint16Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uint16 value: %w", err)
	case ydb_types.TypeUint32:
		var value uint32
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Uint32Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uint32 value: %w", err)
	case ydb_types.TypeUint64:
		var value uint64
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Uint64Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uint64 value: %w", err)
	case ydb_types.TypeFloat:
		var value float32
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.FloatValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to float value: %w", err)
	case ydb_types.TypeDouble:
		var value float64
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.DoubleValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to double value: %w", err)
	case ydb_types.TypeDate:
		var value uint32
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.DateValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to date value: %w", err)
	case ydb_types.TypeTimestamp:
		var value uint64
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.TimestampValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to timestamp value: %w", err)
	case ydb_types.TypeInterval:
		var value int64
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.IntervalValueFromMicroseconds(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to interval value: %w", err)
	case ydb_types.TypeTzDate:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.TzDateValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to tz date value: %w", err)
	case ydb_types.TypeTzDatetime:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.TzDatetimeValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to tz datetime value: %w", err)
	case ydb_types.TypeTzTimestamp:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.TzTimestampValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to tz timestamp value: %w", err)
	case ydb_types.TypeString:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.BytesValue([]byte(value)), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to string value: %w", err)
	case ydb_types.TypeUTF8:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.UTF8Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to utf8 value: %w", err)
	case ydb_types.TypeYSON:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.YSONValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to yson value: %w", err)
	case ydb_types.TypeJSON:
		j, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("ConvertToYDBValue: marshal to json value: %w", err)
		}
		return ydb_types.JSONValueFromBytes(j), nil
	case ydb_types.TypeUUID:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			uuid, err := StringToUUID(value)
			if err != nil {
				return nil, fmt.Errorf("ConvertToYDBValue: convert to uuid value: %w", err)
			}
			return ydb_types.UUIDValue(uuid), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uuid value: %w", err)
	}
	return nil, fmt.Errorf("ConvertToYDBValue: unsupported type: %v", t)
}

func CheckPrimaryKeySize(tablePk []string, updatePk []json.RawMessage) error {
	if len(tablePk) != len(updatePk) {
		return fmt.Errorf("CheckPrimaryKeySize: len of primary key is not equal to len of values")
	}
	return nil
}

func GenListParam(ctx context.Context, tableMetaInfo TableMetaInfo, txsData []types.TxData) (ydb_types.Value, error) {
	values := make([]ydb_types.Value, 0, len(txsData))
	for row, txData := range txsData {
		err := CheckPrimaryKeySize(tableMetaInfo.PrimaryKey, txData.KeyValues)
		if err != nil {
			xlog.Error(ctx, "Len of primary key is not equal to len of values",
				zap.Int("len of primary keys", len(tableMetaInfo.PrimaryKey)),
				zap.Int("len of values", len(txData.KeyValues)))
			return nil, fmt.Errorf("GenListParam: %w", err)
		}
		ydbColumns := make([]ydb_types.StructValueOption, 0, len(txData.KeyValues)+len(txData.ColumnValues))
		for i := 0; i < len(txData.KeyValues); i++ {
			columnName := tableMetaInfo.PrimaryKey[i]
			value, err := ConvertToYDBValue(txData.KeyValues[i], tableMetaInfo.Columns[columnName].Type)
			if err != nil {
				xlog.Error(ctx, "Can't get ydb value for column", zap.String("column", columnName), zap.Int("row", row))
				return nil, fmt.Errorf("GenListParam for column %s row %d : %w", columnName, row, err)
			}
			ydbColumns = append(ydbColumns, ydb_types.StructFieldValue(columnName, value))
		}
		for columnName, columnValue := range txData.ColumnValues {
			value, err := ConvertToYDBValue(columnValue, tableMetaInfo.Columns[columnName].Type)
			if err != nil {
				xlog.Error(ctx, "Can't get ydb value for column", zap.String("column", columnName), zap.Int("row", row))
				return nil, fmt.Errorf("GenListParam for column %s row %d : %w", columnName, row, err)
			}
			ydbColumns = append(ydbColumns, ydb_types.StructFieldValue(columnName, value))
		}

		values = append(values, ydb_types.StructValue(ydbColumns...))
	}

	xlog.Debug(ctx, "Gen param for statement", zap.Int("rows_count", len(txsData)), zap.Int("param_list_size", len(values)))
	return ydb_types.ListValue(values...), nil
}

func GenQueryFromUpdateTx(ctx context.Context, tableMetaInfo TableMetaInfo, txData []types.TxData, localStatementNum int, globalStatementNum int) (QueryStatement, error) {
	result := NewQueryStatement()
	pName := "$p_" + string(fmt.Sprint(globalStatementNum)) + "_" + string(fmt.Sprint(localStatementNum))
	result.Statement = "UPSERT INTO " + tableMetaInfo.Name + " SELECT * FROM AS_TABLE(" + pName + ");\n"
	param, err := GenListParam(ctx, tableMetaInfo, txData)
	if err != nil {
		xlog.Error(ctx, "Unable to gen list param", zap.Error(err))
		return QueryStatement{}, fmt.Errorf("GenQueryFromUpdateTx: %w", err)
	}
	result.Params = []table.ParameterOption{table.ValueParam(pName, param)}
	return *result, nil
}

func GenQueryFromEraseTx(ctx context.Context, tableMetaInfo TableMetaInfo, txData []types.TxData, localStatementNum int, globalStatementNum int) (QueryStatement, error) {
	result := NewQueryStatement()
	pName := "$p_" + string(fmt.Sprint(globalStatementNum)) + "_" + string(fmt.Sprint(localStatementNum))
	result.Statement = "DELETE FROM " + tableMetaInfo.Name + " ON SELECT * FROM AS_TABLE(" + pName + ");\n"
	param, err := GenListParam(ctx, tableMetaInfo, txData)
	if err != nil {
		xlog.Error(ctx, "Unable to gen list param", zap.Error(err))
		return QueryStatement{}, fmt.Errorf("GenQueryFromEraseTx: %w", err)
	}
	result.Params = []table.ParameterOption{table.ValueParam(pName, param)}
	return *result, nil
}

func GenQueryByTxsType(ctx context.Context, tableMetaInfo TableMetaInfo, txData []types.TxData, globalStatementNum int) (QueryStatement, error) {
	result := NewQueryStatement()
	updateTxs := make([]types.TxData, 0)
	eraseTxs := make([]types.TxData, 0)
	if len(txData) == 0 {
		xlog.Info(ctx, "list of txs for gen query is empty")
		return QueryStatement{}, nil
	}
	statementNum := 0
	lastTxIsUpdate := txData[0].IsUpdateOperation()
	for i := range txData {
		if txData[i].IsUpdateOperation() {
			updateTxs = append(updateTxs, txData[i])
			if !lastTxIsUpdate {
				txQuery, err := GenQueryFromEraseTx(ctx, tableMetaInfo, eraseTxs, statementNum, globalStatementNum)
				statementNum++
				if err != nil {
					xlog.Error(ctx, "error in gen query for erase txs", zap.Error(err))
					return QueryStatement{}, fmt.Errorf("GenQuery: %w", err)
				}
				result.Statement += txQuery.Statement
				result.Params = append(result.Params, txQuery.Params...)
				eraseTxs = eraseTxs[:0]
			}
			lastTxIsUpdate = true
			continue
		}
		if txData[i].IsEraseOperation() {
			eraseTxs = append(eraseTxs, txData[i])
			if lastTxIsUpdate {
				txQuery, err := GenQueryFromUpdateTx(ctx, tableMetaInfo, updateTxs, statementNum, globalStatementNum)
				statementNum++
				if err != nil {
					xlog.Error(ctx, "error in gen query for update txs", zap.Error(err))
					return QueryStatement{}, fmt.Errorf("GenQuery: %w", err)
				}
				result.Statement += txQuery.Statement
				result.Params = append(result.Params, txQuery.Params...)
				updateTxs = updateTxs[:0]
			}
			lastTxIsUpdate = false
			continue
		}
		return QueryStatement{}, fmt.Errorf("GenQuery: unknown tx operation type")
	}

	if len(updateTxs) > 0 {
		txQuery, err := GenQueryFromUpdateTx(ctx, tableMetaInfo, updateTxs, statementNum, globalStatementNum)
		if err != nil {
			xlog.Error(ctx, "error in gen query for update txs", zap.Error(err))
			return QueryStatement{}, fmt.Errorf("GenQuery: %w", err)
		}
		result.Statement += txQuery.Statement
		result.Params = append(result.Params, txQuery.Params...)
	}
	if len(eraseTxs) > 0 {
		txQuery, err := GenQueryFromEraseTx(ctx, tableMetaInfo, eraseTxs, statementNum, globalStatementNum)
		if err != nil {
			xlog.Error(ctx, "error in gen query for erase txs", zap.Error(err))
			return QueryStatement{}, fmt.Errorf("GenQuery: %w", err)
		}
		result.Statement += txQuery.Statement
		result.Params = append(result.Params, txQuery.Params...)
	}

	return *result, nil
}

func GenQuery(ctx context.Context, tableMetaInfo TableMetaInfo, txData []types.TxData, globalStatementNum int) (PushQuery, error) {
	var result PushQuery
	txQuery, err := GenQueryByTxsType(ctx, tableMetaInfo, txData, globalStatementNum)
	if err != nil {
		xlog.Error(ctx, "error in gen query for all txs", zap.Error(err))
		return PushQuery{}, fmt.Errorf("GenQuery: %w", err)
	}
	result.Query = txQuery.Statement
	result.Parameters = *table.NewQueryParameters(txQuery.Params...)
	xlog.Debug(ctx, "Gen multi query", zap.String("query", result.Query), zap.String("params", result.Parameters.String()))
	return result, nil
}
