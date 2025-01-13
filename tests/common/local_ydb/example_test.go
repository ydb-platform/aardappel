package local_ydb

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"testing"
)

type SimpleTestSuite struct {
	suite.Suite

	localYdb Ydb
}

func TestSimple(t *testing.T) {
	suite.Run(t, new(SimpleTestSuite))
}

func (suite *SimpleTestSuite) SetupSuite() {
	ydbSettings := NewYdbSettings()
	ydbSettings.OnDisk = true
	localYdb := StartupYdb(suite.T(), *ydbSettings)
	suite.localYdb = localYdb
}

func (suite *SimpleTestSuite) TestClient_Simple() {
	ctx := context.Background()
	db := suite.localYdb.ConnectToDb(suite.T(), ctx)
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		_, err := tx.Execute(ctx, "SELECT * FROM test;", nil)
		return err
	})
	require.Error(suite.T(), err)

	client := db.Table()
	err = client.Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, "CREATE TABLE test (id UINT32, PRIMARY KEY(id))", nil)
	})
	assert.NoError(suite.T(), err)

	suite.localYdb.RestartWithDowntime(suite.T(), ctx, 3)

	err = db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		_, err := tx.Execute(ctx, "SELECT * FROM test;", nil)
		return err
	})
	require.NoErrorf(suite.T(), err, "Failed to execute test query")
}
