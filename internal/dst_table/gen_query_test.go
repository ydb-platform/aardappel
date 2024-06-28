package dst_table

import (
	"aardappel/internal/reader"
	"aardappel/internal/types"
	"context"
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
	}
	return result
}

func TestDefferentLensOfKeys(t *testing.T) {
	ctx := context.Background()
	txData, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"15\", \"value2\":15, \"value3\":null},\"key\":[15 ],\"ts\":[1718408057080,18446744073709551615]}"))
	_, err := GenQueryFromUpdateTx(ctx, GetTestTableMetaInfo(), []types.TxData{txData}, 0, 0)
	require.NotNil(t, err)
	targetErr := "GenQueryFromUpdateTx: GenListParam: CheckPrimaryKeySize: len of primary key is not equal to len of values"
	assert.Equal(t, targetErr, err.Error())
}

func TestGenUpdateQuery(t *testing.T) {
	ctx := context.Background()
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"15\", \"value2\":18446744073709551615, \"value3\":1.00000009, \"value4\":null},\"key\":[15,\"15\"],\"ts\":[18446744073709551615,18446744073709551615]}"))
	txData2, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"15\", \"value3\":1.00000009},\"key\":[16,\"16\"],\"ts\":[18446744073709551614,18446744073709551614]}"))
	query, err := GenQueryFromUpdateTx(ctx, GetTestTableMetaInfo(), []types.TxData{txData1, txData2}, 0, 1)
	require.Nil(t, err)
	assert.Equal(t, "UPSERT INTO path SELECT * FROM AS_TABLE($p_1_0);\n", query.Statement)
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
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[15,\"15\"],\"ts\":[18446744073709551615,18446744073709551615]}"))
	txData2, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[16,\"16\"],\"ts\":[18446744073709551614,18446744073709551614]}"))
	query, err := GenQueryFromEraseTx(ctx, GetTestTableMetaInfo(), []types.TxData{txData1, txData2}, 1, 1)
	require.Nil(t, err)
	assert.Equal(t, "DELETE FROM path ON SELECT * FROM AS_TABLE($p_1_1);\n", query.Statement)
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
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"15\", \"value2\":18446744073709551615, \"value3\":1.00000009, \"value4\":null},\"key\":[15,\"15\"],\"ts\":[18446744073709551615,18446744073709551615]}"))
	txData2, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"15\", \"value3\":1.00000009},\"key\":[16,\"16\"],\"ts\":[18446744073709551614,18446744073709551614]}"))
	txData3, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[15,\"15\"],\"ts\":[18446744073709551615,18446744073709551615]}"))
	txData4, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[16,\"16\"],\"ts\":[18446744073709551614,18446744073709551614]}"))
	txData5, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"15\", \"value3\":1.00000009},\"key\":[17,\"17\"],\"ts\":[18446744073709551613,18446744073709551613]}"))
	query, err := GenQuery(ctx, GetTestTableMetaInfo(), []types.TxData{txData1, txData2, txData3, txData4, txData5}, 0)
	require.Nil(t, err)
	expectedResult := "UPSERT INTO path SELECT * FROM AS_TABLE($p_0_0);\n" +
		"DELETE FROM path ON SELECT * FROM AS_TABLE($p_0_1);\n" +
		"UPSERT INTO path SELECT * FROM AS_TABLE($p_0_2);\n"
	assert.Equal(t, expectedResult, query.Query)
	expectedParams := "{\"$p_0_0\":" +
		"[" +
		"<|`key1`:15,`key2`:\"15\",`value1`:\"15\",`value2`:18446744073709551615ul,`value3`:Just(Double(\"1.00000009\")),`value4`:Nothing(Optional<String>)|>," +
		"<|`key1`:16,`key2`:\"16\",`value1`:\"15\",`value3`:Just(Double(\"1.00000009\"))|>" +
		"]," +
		"\"$p_0_1\":" +
		"[" +
		"<|`key1`:15,`key2`:\"15\"|>," +
		"<|`key1`:16,`key2`:\"16\"|>" +
		"]," +
		"\"$p_0_2\":[<|`key1`:17,`key2`:\"17\",`value1`:\"15\",`value3`:Just(Double(\"1.00000009\"))|>]}"
	assert.Equal(t, expectedParams, query.Parameters.String())
}
