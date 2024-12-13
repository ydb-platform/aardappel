package dst_table

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"sort"
	"strings"
	"time"
)

type UpdatingData struct {
	ColumnValues    map[string]json.RawMessage
	ColumnsString   string
	KeyValues       []json.RawMessage
	KeyValuesString string
}

func (data *UpdatingData) GetSortUpdatingColumns() string {
	sortColumns := make([]string, 0, len(data.ColumnValues))
	for key := range data.ColumnValues {
		sortColumns = append(sortColumns, fmt.Sprintf("`%s`", key))
	}
	sort.Strings(sortColumns)

	result := strings.Join(sortColumns, ", ")

	return result
}

func NewUpdatingData(txData types.TxData, txKey string) *UpdatingData {
	var data UpdatingData
	data.ColumnValues = make(map[string]json.RawMessage, len(txData.ColumnValues))
	for columnName, columnValue := range txData.ColumnValues {
		data.ColumnValues[columnName] = columnValue
	}
	data.KeyValues = txData.KeyValues
	data.KeyValuesString = txKey
	data.ColumnsString = data.GetSortUpdatingColumns()
	return &data
}

func (data *UpdatingData) UpdateColumns(txData types.TxData, txKey string) {
	for columnName, columnValue := range txData.ColumnValues {
		data.ColumnValues[columnName] = columnValue
	}
	data.KeyValues = txData.KeyValues
	data.KeyValuesString = txKey
	data.ColumnsString = data.GetSortUpdatingColumns()
}

type ByColumns []UpdatingData

func (v ByColumns) Len() int {
	return len(v)
}

func (v ByColumns) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

func (v ByColumns) Less(i, j int) bool {
	return (v[i].ColumnsString < v[j].ColumnsString) ||
		(v[i].ColumnsString == v[j].ColumnsString && v[i].KeyValuesString < v[j].KeyValuesString)
}

type ByKeys []UpdatingData

func (v ByKeys) Len() int {
	return len(v)
}

func (v ByKeys) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

func (v ByKeys) Less(i, j int) bool {
	return v[i].KeyValuesString < v[j].KeyValuesString
}

func groupByColumns(data []UpdatingData) [][]UpdatingData {
	var result [][]UpdatingData
	if len(data) == 0 {
		return result
	}

	currentColumns := data[0].ColumnsString
	var group []UpdatingData

	for _, tx := range data {
		if tx.ColumnsString != currentColumns {
			result = append(result, group)
			group = []UpdatingData{}
			currentColumns = tx.ColumnsString
		}
		group = append(group, tx)
	}
	result = append(result, group)

	return result
}

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
		var value_str string
		if err = json.Unmarshal(v, &value_str); err != nil {
			return nil, fmt.Errorf("ConvertToYDBValue: timestamp: unmurshal to string value: %w", err)
		}
		format := "2006-01-02T15:04:05.000000Z"
		value, err := time.Parse(format, value_str)
		if err != nil {
			return nil, fmt.Errorf("ConvertToYDBValue: timestamp: error to time parse: %w", err)
		}
		return ydb_types.TimestampValue(uint64(value.UnixMicro())), nil
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
			data, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return nil, fmt.Errorf("DecodeString: unable to decode base64: %w", err)
			}
			return ydb_types.BytesValue(data), nil
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
			u, err := uuid.Parse(value)
			if err != nil {
				return nil, fmt.Errorf("ConvertToYDBValue: convert to uuid value: %w", err)
			}
			return ydb_types.UuidValue(u), nil
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

func GenListParam(ctx context.Context, tableMetaInfo TableMetaInfo, txsData []UpdatingData) (ydb_types.Value, error) {
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
			_, columnExist := tableMetaInfo.Columns[columnName]
			if !columnExist {
				return nil, fmt.Errorf("Column [%s] is not in dst table scheme", columnName)
			}
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

func GenQueryFromUpdateTx(ctx context.Context, tableMetaInfo TableMetaInfo, txData []UpdatingData, localStatementNum int, globalStatementNum int) (QueryStatement, error) {
	result := NewQueryStatement()
	pName := "$p_" + string(fmt.Sprint(globalStatementNum)) + "_" + string(fmt.Sprint(localStatementNum))

	quotedKeys := make([]string, len(tableMetaInfo.PrimaryKey))
	for i, key := range tableMetaInfo.PrimaryKey {
		quotedKeys[i] = fmt.Sprintf("`%s`", key)
	}
	allColumns := strings.Join(quotedKeys, ", ")

	if len(txData[0].ColumnValues) > 0 {
		allColumns += ", " + txData[0].ColumnsString
	}
	result.Statement = "UPSERT INTO `" + tableMetaInfo.Name + "` (" + allColumns + ") SELECT " + allColumns + " FROM AS_TABLE(" + pName + ");\n"
	param, err := GenListParam(ctx, tableMetaInfo, txData)
	if err != nil {
		xlog.Error(ctx, "Unable to gen list param", zap.Error(err))
		return QueryStatement{}, fmt.Errorf("GenQueryFromUpdateTx: %w", err)
	}
	result.Params = []table.ParameterOption{table.ValueParam(pName, param)}
	return *result, nil
}

func GenQueryFromEraseTx(ctx context.Context, tableMetaInfo TableMetaInfo, txData []UpdatingData, localStatementNum int, globalStatementNum int) (QueryStatement, error) {
	result := NewQueryStatement()
	pName := "$p_" + string(fmt.Sprint(globalStatementNum)) + "_" + string(fmt.Sprint(localStatementNum))
	result.Statement = "DELETE FROM `" + tableMetaInfo.Name + "` ON SELECT * FROM AS_TABLE(" + pName + ");\n"
	param, err := GenListParam(ctx, tableMetaInfo, txData)
	if err != nil {
		xlog.Error(ctx, "Unable to gen list param", zap.Error(err))
		return QueryStatement{}, fmt.Errorf("GenQueryFromEraseTx: %w", err)
	}
	result.Params = []table.ParameterOption{table.ValueParam(pName, param)}
	return *result, nil
}

func GenQueryByTxsType(ctx context.Context, tableMetaInfo TableMetaInfo, txData []types.TxData, globalStatementNum int) (QueryStatement, error) {
	if len(txData) == 0 {
		xlog.Debug(ctx, "List of txs to generate query is empty")
		return QueryStatement{}, nil
	}

	upsertResult := make(map[string]UpdatingData)
	deleteResult := make(map[string]UpdatingData)

	serializeKey := func(key []json.RawMessage) (string, error) {
		data, err := json.Marshal(key)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	for i := range txData {
		txKey, err := serializeKey(txData[i].KeyValues)
		if err != nil {
			xlog.Error(ctx, "error to serialize key values in tx", zap.Error(err))
			return QueryStatement{}, fmt.Errorf("GenQueryByTxsType: %w", err)
		}

		if txData[i].IsUpdateOperation() {
			_, deleteExist := deleteResult[txKey]
			if deleteExist {
				delete(deleteResult, txKey)
			}
			if data, upsertExist := upsertResult[txKey]; upsertExist {
				data.UpdateColumns(txData[i], txKey)
			} else {
				upsertResult[txKey] = *NewUpdatingData(txData[i], txKey)
			}
			continue
		}
		if txData[i].IsEraseOperation() {
			_, upsertExist := upsertResult[txKey]
			if upsertExist {
				delete(upsertResult, txKey)
			}
			if data, deleteExist := deleteResult[txKey]; deleteExist {
				data.UpdateColumns(txData[i], txKey)
			} else {
				deleteResult[txKey] = *NewUpdatingData(txData[i], txKey)
			}
			continue
		}
		return QueryStatement{}, fmt.Errorf("GenQuery: unknown tx operation type")
	}

	upsertResultList := make([]UpdatingData, 0, len(upsertResult))
	deleteResultList := make([]UpdatingData, 0, len(deleteResult))
	for _, data := range upsertResult {
		upsertResultList = append(upsertResultList, data)
	}
	for _, data := range deleteResult {
		deleteResultList = append(deleteResultList, data)
	}
	sort.Sort(ByColumns(upsertResultList))
	sort.Sort(ByKeys(deleteResultList))

	upsertResults := groupByColumns(upsertResultList)

	result := NewQueryStatement()
	localStatementNum := 0

	if len(upsertResultList) > 0 {
		for i := range upsertResults {
			upsertTxQuery, err := GenQueryFromUpdateTx(ctx, tableMetaInfo, upsertResults[i], localStatementNum, globalStatementNum)
			if err != nil {
				xlog.Error(ctx, "error in gen query for upsert txs", zap.Error(err))
				return QueryStatement{}, fmt.Errorf("GenQueryByTxsType: %w", err)
			}
			result.Statement += upsertTxQuery.Statement
			result.Params = append(result.Params, upsertTxQuery.Params...)
			localStatementNum++
		}
	}

	if len(deleteResultList) > 0 {
		deleteTxQuery, err := GenQueryFromEraseTx(ctx, tableMetaInfo, deleteResultList, localStatementNum, globalStatementNum)
		if err != nil {
			xlog.Error(ctx, "error in gen query for erase txs", zap.Error(err))
			return QueryStatement{}, fmt.Errorf("GenQueryByTxsType: %w", err)
		}
		result.Statement += deleteTxQuery.Statement
		result.Params = append(result.Params, deleteTxQuery.Params...)
		localStatementNum++
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
	result.ModificationsCount = len(txData)
	xlog.Debug(ctx, "Gen multi query", zap.String("query", result.Query), zap.String("params", result.Parameters.String()))
	return result, nil
}
