package common

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"log"
	"os"
	"testing"
	"time"
)

const (
	port         = "2136"
	dockerDBName = "/local"
	localUIAddr  = "localhost:8765"
	uiPort       = "8765/tcp" // for debugging

	localYdbEnv     = "LOCAL_YDB_ADDR"
	defaultYdbImage = "ydbplatform/local-ydb:latest"

	containerStopTimeout = 120 * time.Second
)

type ContainerSettings struct {
	YdbImage string
}

func SetupContainerSettings() ContainerSettings {
	ydbImage := os.Getenv("YDB_IMAGE")
	if ydbImage == "" {
		ydbImage = defaultYdbImage
	}
	fmt.Println("YDB_IMAGE:", ydbImage)
	return ContainerSettings{YdbImage: ydbImage}
}

type YdbConfig struct {
	Endpoint string
	Database string
}

type YDBContainer struct {
	container testcontainers.Container
	UIAddr    string
	YdbConfig YdbConfig
}

func (y *YDBContainer) Terminate() error {
	if y.container == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), containerStopTimeout)
	defer cancel()
	return y.container.Terminate(cleanupCtx)
}

func StartYdb(ctx context.Context, t testing.TB) (
	*YDBContainer,
	error,
) {
	t.Helper()

	ydbContainer, err := newYDBContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(
		func() {
			require.NoError(t, ydbContainer.Terminate())
		},
	)

	return ydbContainer, nil
}

func newYDBContainer(ctx context.Context) (*YDBContainer, error) {
	var addr string
	var container testcontainers.Container
	var uiAddr string

	containerSettings := SetupContainerSettings()

	if envAddr, ok := os.LookupEnv(localYdbEnv); ok {
		addr = envAddr
		uiAddr = localUIAddr
	} else {
		env := map[string]string{
			"GRPC_PORT":                port,
			"YDB_DEFAULT_LOG_LEVEL":    "NOTICE",
			"YDB_USE_IN_MEMORY_PDISKS": "true",
			"YDB_FEATURE_FLAGS":        "enable_implicit_query_parameter_types",
		}
		exposedPort, _ := nat.NewPort("tcp", port)
		req := testcontainers.ContainerRequest{
			Image:        containerSettings.YdbImage,
			ExposedPorts: []string{exposedPort.Port(), uiPort},
			WaitingFor: wait.ForAll(
				wait.ForHealthCheck().WithStartupTimeout(time.Minute * 5),
			),
			Env: env,
		}

		container, err := testcontainers.GenericContainer(
			ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: req,
				Started:          true,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("error creating YDB test container: %w", err)
		}

		addr, err = container.PortEndpoint(ctx, exposedPort, "")
		if err != nil {
			return nil, fmt.Errorf("error getting YDB port: %w", err)
		}
		uiAddr, err = container.PortEndpoint(ctx, uiPort, "")
		if err != nil {
			return nil, fmt.Errorf("error getting YDB endpoint: %w", err)
		}
	}

	return &YDBContainer{
		container: container,
		UIAddr:    uiAddr,
		YdbConfig: YdbConfig{
			Endpoint: addr,
			Database: dockerDBName,
		},
	}, nil
}

func SetupYdb(t *testing.T) *ydb.Driver {
	ctx := context.Background()
	ydbContainer, err := StartYdb(ctx, t)
	assert.NoErrorf(t, err, "Failed to start YDB container")

	dsn := fmt.Sprintf("grpc://%s", ydbContainer.YdbConfig.Endpoint)
	db, err := ydb.Open(ctx, dsn,
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithDatabase(ydbContainer.YdbConfig.Database))
	assert.NoErrorf(t, err, "Failed to connect to YDB")

	query := "SELECT 1;"
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = db.Table().DoTx(ctxWithTimeout, func(ctx context.Context, tx table.TransactionActor) error {
		_, err := tx.Execute(ctx, query, nil)
		return err
	})
	if err != nil {
		t.Fatalf("test query failed: %v", err)
	}
	log.Println("Connected to YDB successfully!")
	log.Println("Connect to UI: " + ydbContainer.UIAddr)
	return db
}
