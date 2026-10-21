package feed

import (
	"context"
	"fmt"
	"testing"
	"time"

	"aardappel/internal/config"
	"aardappel/internal/processor"
	"aardappel/internal/util/ydb/ydb_locker"
	"aardappel/tests/common/local_ydb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.uber.org/zap"
)

func startupTestConfig(t *testing.T) (context.Context, config.Config, *ydb.Driver) {
	t.Helper()
	local := local_ydb.StartupYdb(t, *local_ydb.NewYdbSettings())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	db := local.ConnectToDb(t, ctx)
	t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
	suffix := uuid.New().String()[:8]
	target := "/local/target_" + suffix
	state := "state_" + suffix
	require.NoError(t, db.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		return session.ExecuteSchemeQuery(ctx, fmt.Sprintf("CREATE TABLE `%s` (id Uint64, PRIMARY KEY (id))", target))
	}))
	cfg := config.Config{
		Clusters: []config.Cluster{
			{ID: "source", ConnectionString: "grpc://unreachable.invalid:2136/local"},
			{ID: "destination", ConnectionString: local.ConnectionString(), StaticToken: "test-token"},
		},
		WaitForFeed: true,
	}
	for _, id := range []string{"first", "second"} {
		cfg.Feeds = append(cfg.Feeds, config.Feed{
			ID: id, InstanceId: id, Source: "source", Destination: "destination",
			StateTable: state, MaxExpHbInterval: 5,
			Streams: []config.Stream{{SrcTopic: "not-opened-during-preparation", DstTable: target, Consumer: "consumer"}},
		})
	}
	return ctx, cfg, db
}

func TestPrepareFeedsHappyPath(t *testing.T) {
	ctx, cfg, _ := startupTestConfig(t)
	candidates, connections := prepareFeeds(ctx, zap.NewNop(), cfg, "test-owner")
	t.Cleanup(func() {
		for _, connection := range connections {
			require.NoError(t, connection.Client.GetDriver().Close(context.Background()))
		}
	})
	require.Len(t, connections, 1)
	require.Contains(t, connections, "destination")
	require.NotContains(t, connections, "source")
	require.Len(t, candidates, 2)
	destination := connections["destination"].Client.GetDriver()
	for i, candidate := range candidates {
		require.Equal(t, cfg.Feeds[i], candidate.Feed)
		require.Equal(t, candidate.Feed.InstanceId, candidate.Locker.LockName)
		require.Equal(t, "test-owner", candidate.Locker.OwnerName)
		require.Equal(t, 10*time.Second, candidate.Locker.Ttl)
		require.Same(t, destination, candidate.Locker.LockStorage.(*ydb_locker.YdbLockStorage).Db)
	}
	require.NoError(t, destination.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		_, result, err := session.Execute(ctx, table.DefaultTxControl(),
			fmt.Sprintf("SELECT id, step_id, tx_id, state, stage FROM `%s` ORDER BY id", cfg.Feeds[0].StateTable), nil)
		if err != nil {
			return err
		}
		defer result.Close()
		require.True(t, result.NextResultSet(ctx))
		for _, id := range []string{"first", "second"} {
			require.True(t, result.NextRow())
			var gotID, state, stage string
			var step, tx uint64
			require.NoError(t, result.ScanWithDefaults(&gotID, &step, &tx, &state, &stage))
			require.Equal(t, id, gotID)
			require.Zero(t, step)
			require.Zero(t, tx)
			require.Equal(t, processor.REPLICATION_OK, state)
			require.Equal(t, processor.STAGE_INITIAL_SCAN, stage)
		}
		require.False(t, result.NextRow())
		return result.Err()
	}))
}

func TestPrepareFeedsSkipsUnhealthyFeeds(t *testing.T) {
	ctx, base, _ := startupTestConfig(t)
	tests := []struct {
		name      string
		configure func(*config.Config)
	}{
		{
			name: "missing destination table",
			configure: func(cfg *config.Config) {
				cfg.Feeds[0].Streams[0].DstTable += "_missing"
			},
		},
		{
			name: "invalid destination authentication",
			configure: func(cfg *config.Config) {
				destination := cfg.Clusters[1]
				destination.ID = "invalid-auth"
				destination.StaticToken = ""
				cfg.Clusters = append(cfg.Clusters, destination)
				cfg.Feeds[0].Destination = destination.ID
			},
		},
		{
			name: "state table cannot be created",
			configure: func(cfg *config.Config) {
				// A table cannot contain another table.
				cfg.Feeds[0].StateTable = "`" + cfg.Feeds[0].Streams[0].DstTable + "/state`"
			},
		},
		{
			name: "state row cannot be initialized",
			configure: func(cfg *config.Config) {
				// The existing target has a Uint64 key and no replication state columns.
				cfg.Feeds[0].StateTable = "`" + cfg.Feeds[0].Streams[0].DstTable + "`"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			cfg.Clusters = append([]config.Cluster(nil), base.Clusters...)
			cfg.Feeds = append([]config.Feed(nil), base.Feeds...)
			for i := range cfg.Feeds {
				cfg.Feeds[i].Streams = append([]config.Stream(nil), base.Feeds[i].Streams...)
			}
			tt.configure(&cfg)
			candidates, connections := prepareFeeds(ctx, zap.NewNop(), cfg, "test-owner")
			t.Cleanup(func() {
				for _, connection := range connections {
					require.NoError(t, connection.Client.GetDriver().Close(context.Background()))
				}
			})
			require.Len(t, candidates, 1)
			require.Equal(t, cfg.Feeds[1], candidates[0].Feed)
			require.Len(t, connections, 1)
			require.Contains(t, connections, "destination")
			require.NotContains(t, connections, "source")
			// Skipping an unhealthy feed must leave the shared destination usable.
			require.NoError(t, connections["destination"].Client.GetDriver().Table().Do(ctx, func(ctx context.Context, session table.Session) error {
				_, err := session.DescribeTable(ctx, cfg.Feeds[1].Streams[0].DstTable)
				return err
			}))
		})
	}
}
