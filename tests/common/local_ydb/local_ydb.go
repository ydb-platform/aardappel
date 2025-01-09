package local_ydb

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"log"
	"os"
	"testing"
	"time"
)

const (
	containerDBName  = "/local"
	localYdbEndpoint = "LOCAL_YDB_ENDPOINT"
	dbName           = "DB_NAME"
)

type YdbSettings struct {
	YdbImage *string
	OnDisk   bool
}

func NewYdbSettings() *YdbSettings {
	return &YdbSettings{
		OnDisk: false,
	}
}

type YdbInfo struct {
	GrpcEndpoint string
	UIEndpoint   string
	Database     string
}

type Ydb struct {
	ydbContainer *YDBContainer
	ydbInfo      YdbInfo
}

func NewYDB(ctx context.Context, ydbSettings YdbSettings) (*Ydb, error) {
	if envEndpoint, ok := os.LookupEnv(localYdbEndpoint); ok {
		if envDBName, ok := os.LookupEnv(dbName); ok {
			return &Ydb{
				ydbContainer: nil,
				ydbInfo: YdbInfo{
					GrpcEndpoint: envEndpoint,
					Database:     envDBName,
				},
			}, nil
		} else {
			return nil, fmt.Errorf("error create YDB: DB_NAME is not set")
		}
	}

	if _, ok := os.LookupEnv(dbName); ok {
		log.Println("DB_NAME is ignored")
	}

	ydbContainer, err := NewYDBContainer(ctx, ydbSettings)
	if err != nil {
		return nil, fmt.Errorf("error create YDB: %w", err)
	}

	return &Ydb{
		ydbContainer: ydbContainer,
		ydbInfo: YdbInfo{
			GrpcEndpoint: ydbContainer.ConnectInfo.GrpcEndpoint,
			UIEndpoint:   ydbContainer.ConnectInfo.UIEndpoint,
			Database:     containerDBName,
		},
	}, nil
}

func (localYdb *Ydb) Stop(t *testing.T, ctx context.Context) {
	err := localYdb.ydbContainer.Terminate(ctx)
	require.NoErrorf(t, err, "Failed to stop YDB container: %w", err)
}

func (localYdb *Ydb) Restart(t *testing.T, ctx context.Context) {
	err := localYdb.ydbContainer.Restart(ctx)
	require.NoErrorf(t, err, "Failed to restart YDB container: %w", err)
}

func (localYdb *Ydb) RestartWithDowntime(t *testing.T, ctx context.Context, downtimeSec int) {
	err := localYdb.ydbContainer.RestartWithDowntime(ctx, downtimeSec)
	require.NoErrorf(t, err, "Failed to restart YDB container: %w", err)
}

func (localYdb *Ydb) ConnectToDb(t *testing.T, ctx context.Context) *ydb.Driver {
	dsn := fmt.Sprintf("grpc://%s", localYdb.ydbInfo.GrpcEndpoint)
	db, err := ydb.Open(ctx, dsn,
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithDatabase(localYdb.ydbInfo.Database))
	require.NoErrorf(t, err, "Failed to connect to YDB: %w", err)
	return db
}

func StartYdb(t *testing.T, ctx context.Context, ydbSettings YdbSettings) Ydb {
	//t.Helper()
	localYdb, err := NewYDB(ctx, ydbSettings)
	require.NoErrorf(t, err, "Failed to start YDB container: %w", err)

	t.Cleanup(
		func() {
			localYdb.Stop(t, ctx)
		},
	)
	return *localYdb
}

func StartupYdb(t *testing.T, ydbSettings YdbSettings) Ydb {
	ctx := context.Background()
	localYdb := StartYdb(t, ctx, ydbSettings)

	db := localYdb.ConnectToDb(t, ctx)

	query := "SELECT 1;"
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err := db.Table().DoTx(ctxWithTimeout, func(ctx context.Context, tx table.TransactionActor) error {
		_, err := tx.Execute(ctx, query, nil)
		return err
	})
	require.NoErrorf(t, err, "Failed to execute test query")

	log.Println("Connected to YDB successfully!")
	if localYdb.ydbInfo.UIEndpoint != "" {
		log.Println("Connect to UI: " + localYdb.ydbInfo.UIEndpoint)
	}
	return localYdb
}
