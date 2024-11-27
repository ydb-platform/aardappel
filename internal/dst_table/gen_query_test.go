package dst_table

import (
	"aardappel/internal/types"
	"aardappel/internal/util/reader"
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"testing"
)

func GetTestTableMetaInfo() TableMetaInfo {
	var result TableMetaInfo
	result.Name = "path"
	result.PrimaryKey = []string{"key1", "key2"}
	result.Columns = map[string]options.Column{
		"key1":   options.Column{"key1", ydb_types.TypeInt32, ""},
		"key2":   options.Column{"key1", ydb_types.TypeString, ""},
		"value1": options.Column{"value1", ydb_types.TypeString, ""},
		"value2": options.Column{"value2", ydb_types.TypeUint64, ""},
		"value3": options.Column{"value3", ydb_types.Optional(ydb_types.TypeDouble), ""},
		"value4": options.Column{"value4", ydb_types.Optional(ydb_types.TypeString), ""},
		"value5": options.Column{"value5", ydb_types.Optional(ydb_types.TypeTimestamp), ""},
	}
	return result
}

func CreateData(txData types.TxData) UpdatingData {
	keyValues, err := json.Marshal(txData.KeyValues)
	if err != nil {
		panic(err)
	}
	return *NewUpdatingData(txData, string(keyValues))
}

func TestDefferentLensOfKeys(t *testing.T) {
	ctx := context.Background()
	txData, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTU=\", \"value2\":15, \"value3\":null},\"key\":[15],\"ts\":[1718408057080,18446744073709551615]}"), 0)
	_, err := GenQueryFromUpdateTx(ctx, GetTestTableMetaInfo(), []UpdatingData{CreateData(txData)}, 0, 0)
	require.NotNil(t, err)
	targetErr := "GenQueryFromUpdateTx: GenListParam: CheckPrimaryKeySize: len of primary key is not equal to len of values"
	assert.Equal(t, targetErr, err.Error())
}

func TestColumnNameNotInScheme(t *testing.T) {
	ctx := context.Background()
	txData, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value10\":\"15\", \"value3\":1.00000009},\"key\":[16,\"MTY=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	_, err := GenQueryFromUpdateTx(ctx, GetTestTableMetaInfo(), []UpdatingData{CreateData(txData)}, 0, 1)
	require.NotNil(t, err)
	targetErr := "GenQueryFromUpdateTx: Column [value10] is not in dst table scheme"
	assert.Equal(t, targetErr, err.Error())
}

func TestGenUpdateQuery(t *testing.T) {
	ctx := context.Background()
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTU=\", \"value2\":18446744073709551615, \"value3\":1.00000009, \"value4\":null},\"key\":[15,\"MTU=\"],\"ts\":[18446744073709551615,18446744073709551615]}"), 0)
	txData2, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTU=\", \"value3\":1.00000009},\"key\":[16,\"MTY=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	query, err := GenQueryFromUpdateTx(ctx, GetTestTableMetaInfo(), []UpdatingData{CreateData(txData1), CreateData(txData2)}, 0, 1)
	require.Nil(t, err)
	assert.Equal(t, "UPSERT INTO `path` (`key1`, `key2`, `value1`, `value2`, `value3`, `value4`) SELECT `key1`, `key2`, `value1`, `value2`, `value3`, `value4` FROM AS_TABLE($p_1_0);\n", query.Statement)
	assert.Equal(t, len(query.Params), 1)
	assert.Equal(t, query.Params[0].Name(), "$p_1_0")
	expectedParams := "[" +
		"<|`key1`:15,`key2`:\"15\",`value1`:\"15\",`value2`:18446744073709551615ul,`value3`:Just(Double(\"1.00000009\")),`value4`:Nothing(Optional<String>)|>," +
		"<|`key1`:16,`key2`:\"16\",`value1`:\"15\",`value3`:Just(Double(\"1.00000009\"))|>" +
		"]"
	assert.Equal(t, expectedParams, query.Params[0].Value().Yql())
}

func TestGenEraseQuery(t *testing.T) {
	ctx := context.Background()
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[15,\"MTU=\"],\"ts\":[18446744073709551615,18446744073709551615]}"), 0)
	txData2, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[16,\"MTY=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	query, err := GenQueryFromEraseTx(ctx, GetTestTableMetaInfo(), []UpdatingData{CreateData(txData1), CreateData(txData2)}, 1, 1)
	require.Nil(t, err)
	assert.Equal(t, "DELETE FROM `path` ON SELECT * FROM AS_TABLE($p_1_1);\n", query.Statement)
	assert.Equal(t, len(query.Params), 1)
	assert.Equal(t, query.Params[0].Name(), "$p_1_1")
	expectedParams := "[" +
		"<|`key1`:15,`key2`:\"15\"|>," +
		"<|`key1`:16,`key2`:\"16\"|>" +
		"]"
	assert.Equal(t, expectedParams, query.Params[0].Value().Yql())
}

func TestGenQuery(t *testing.T) {
	ctx := context.Background()
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTU=\", \"value2\":18446744073709551615, \"value3\":1.00000009, \"value4\":null},\"key\":[15,\"MTU=\"],\"ts\":[18446744073709551615,18446744073709551615]}"), 0)
	txData2, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTY=\", \"value3\":1.00000009},\"key\":[16,\"MTY=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	txData3, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTc=\", \"value3\":1.00000009},\"key\":[17,\"MTc=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	txData4, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[16,\"MTY=\"],\"ts\":[18446744073709551615,18446744073709551615]}"), 0)
	txData5, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[18,\"MTg=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	txData6, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[19,\"MTk=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	txData7, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTk=\", \"value3\":1.00000009},\"key\":[19,\"MTk=\"],\"ts\":[18446744073709551613,18446744073709551613]}"), 0)
	txData8, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"Mjc=\", \"value3\":1.00000009},\"key\":[17,\"MTc=\"],\"ts\":[18446744073709551613,18446744073709551613]}"), 0)
	query, err := GenQuery(ctx, GetTestTableMetaInfo(), []types.TxData{txData1, txData2, txData3, txData4, txData5, txData6, txData7, txData8}, 0)
	require.Nil(t, err)
	expectedResult := "UPSERT INTO `path` (`key1`, `key2`, `value1`, `value2`, `value3`, `value4`) SELECT `key1`, `key2`, `value1`, `value2`, `value3`, `value4` FROM AS_TABLE($p_0_0);\n" +
		"UPSERT INTO `path` (`key1`, `key2`, `value1`, `value3`) SELECT `key1`, `key2`, `value1`, `value3` FROM AS_TABLE($p_0_1);\n" +
		"DELETE FROM `path` ON SELECT * FROM AS_TABLE($p_0_2);\n"
	assert.Equal(t, expectedResult, query.Query)
	expectedParams := "{" +
		"\"$p_0_0\":" +
		"[" +
		"<|`key1`:15,`key2`:\"15\",`value1`:\"15\",`value2`:18446744073709551615ul,`value3`:Just(Double(\"1.00000009\")),`value4`:Nothing(Optional<String>)|>" +
		"]," +
		"\"$p_0_1\":" +
		"[" +
		"<|`key1`:17,`key2`:\"17\",`value1`:\"27\",`value3`:Just(Double(\"1.00000009\"))|>," +
		"<|`key1`:19,`key2`:\"19\",`value1`:\"19\",`value3`:Just(Double(\"1.00000009\"))|>" +
		"]," +
		"\"$p_0_2\":" +
		"[" +
		"<|`key1`:16,`key2`:\"16\"|>," +
		"<|`key1`:18,`key2`:\"18\"|>" +
		"]" +
		"}"
	assert.Equal(t, expectedParams, query.Parameters.String())
}

func TestGenOnlyUpsertQuery(t *testing.T) {
	ctx := context.Background()
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTU=\", \"value2\":18446744073709551615, \"value3\":1.00000009, \"value4\":null},\"key\":[15,\"MTU=\"],\"ts\":[18446744073709551615,18446744073709551615]}"), 0)
	txData2, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTY=\", \"value3\":1.00000009},\"key\":[16,\"MTY=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	txData3, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTc=\", \"value3\":1.00000009},\"key\":[17,\"MTc=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	txData4, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTk=\", \"value3\":1.00000009},\"key\":[19,\"MTk=\"],\"ts\":[18446744073709551613,18446744073709551613]}"), 0)
	txData5, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"Mjc=\", \"value3\":1.00000009},\"key\":[17,\"MTc=\"],\"ts\":[18446744073709551613,18446744073709551613]}"), 0)
	query, err := GenQuery(ctx, GetTestTableMetaInfo(), []types.TxData{txData1, txData2, txData3, txData4, txData5}, 0)
	require.Nil(t, err)
	expectedResult := "UPSERT INTO `path` (`key1`, `key2`, `value1`, `value2`, `value3`, `value4`) SELECT `key1`, `key2`, `value1`, `value2`, `value3`, `value4` FROM AS_TABLE($p_0_0);\n" +
		"UPSERT INTO `path` (`key1`, `key2`, `value1`, `value3`) SELECT `key1`, `key2`, `value1`, `value3` FROM AS_TABLE($p_0_1);\n"
	assert.Equal(t, expectedResult, query.Query)
	expectedParams := "{" +
		"\"$p_0_0\":" +
		"[" +
		"<|`key1`:15,`key2`:\"15\",`value1`:\"15\",`value2`:18446744073709551615ul,`value3`:Just(Double(\"1.00000009\")),`value4`:Nothing(Optional<String>)|>" +
		"]," +
		"\"$p_0_1\":" +
		"[" +
		"<|`key1`:16,`key2`:\"16\",`value1`:\"16\",`value3`:Just(Double(\"1.00000009\"))|>," +
		"<|`key1`:17,`key2`:\"17\",`value1`:\"27\",`value3`:Just(Double(\"1.00000009\"))|>," +
		"<|`key1`:19,`key2`:\"19\",`value1`:\"19\",`value3`:Just(Double(\"1.00000009\"))|>" +
		"]" +
		"}"
	assert.Equal(t, expectedParams, query.Parameters.String())
}

func TestGenOnlyEraseQuery(t *testing.T) {
	ctx := context.Background()
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[16,\"MTY=\"],\"ts\":[18446744073709551615,18446744073709551615]}"), 0)
	txData2, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[17,\"MTc=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	txData3, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[16,\"MTY=\"],\"ts\":[18446744073709551614,18446744073709551614]}"), 0)
	query, err := GenQuery(ctx, GetTestTableMetaInfo(), []types.TxData{txData1, txData2, txData3}, 0)
	require.Nil(t, err)
	expectedResult := "DELETE FROM `path` ON SELECT * FROM AS_TABLE($p_0_0);\n"
	assert.Equal(t, expectedResult, query.Query)
	expectedParams := "{\"$p_0_0\":" +
		"[" +
		"<|`key1`:16,`key2`:\"16\"|>," +
		"<|`key1`:17,`key2`:\"17\"|>" +
		"]}"
	assert.Equal(t, expectedParams, query.Parameters.String())
}

func TestGenQueryWithTimestamp(t *testing.T) {
	ctx := context.Background()
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"MTU=\", \"value5\":\"1970-01-01T00:00:01.000001Z\"},\"key\":[15,\"MTU=\"],\"ts\":[18446744073709551615,18446744073709551615]}"), 0)
	query, err := GenQueryFromUpdateTx(ctx, GetTestTableMetaInfo(), []UpdatingData{CreateData(txData1)}, 0, 1)
	require.Nil(t, err)
	assert.Equal(t, "UPSERT INTO `path` (`key1`, `key2`, `value1`, `value5`) SELECT `key1`, `key2`, `value1`, `value5` FROM AS_TABLE($p_1_0);\n", query.Statement)
	assert.Equal(t, len(query.Params), 1)
	assert.Equal(t, query.Params[0].Name(), "$p_1_0")
	expectedParams := "[" +
		"<|`key1`:15,`key2`:\"15\",`value1`:\"15\",`value5`:Just(Timestamp(\"1970-01-01T00:00:01.000001Z\"))|>" +
		"]"
	assert.Equal(t, expectedParams, query.Params[0].Value().Yql())
}
