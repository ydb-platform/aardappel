package feed

import (
	"context"
	"fmt"
	"os"
	"time"

	"aardappel/internal/auth"
	configInit "aardappel/internal/config"
	"aardappel/internal/dst_table"
	"aardappel/internal/hb_tracker"
	processor "aardappel/internal/processor"
	"aardappel/internal/util/misc"
	"aardappel/internal/util/xlog"
	client "aardappel/internal/util/ydb"
	"aardappel/internal/util/ydb/ydb_locker"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

// AcquiredFeed contains the resources needed to run one owned feed.
type AcquiredFeed struct {
	Feed        configInit.Feed
	Source      *client.YdbClient
	Destination *client.YdbClient
	Locker      *ydb_locker.Locker
	Context     context.Context
	Cleanup     func()
}

// AcquireFeed prepares destinations, acquires ownership, and checks the selected source.
func AcquireFeed(ctx context.Context, logger *zap.Logger, config configInit.Config) *AcquiredFeed {
	ctx, cancel := context.WithCancel(ctx)
	ready := false
	defer func() {
		if !ready {
			cancel()
		}
	}()
	hostname, _ := os.Hostname()
	owner := fmt.Sprintf("lock_%s_%s", hostname, uuid.New().String())
	candidates, connections := prepareFeeds(ctx, logger, config, owner)
	if ctx.Err() != nil {
		return nil
	}
	if len(candidates) == 0 {
		xlog.Fatal(ctx, "No feeds passed startup checks; exiting", zap.Int("configured_feeds", len(config.Feeds)))
	}
	allFeedsHealthy := len(candidates) == len(config.Feeds)
	selected, acquired := SelectAnyFeed(ctx, candidates, allFeedsHealthy && config.WaitForFeed, defaultLockCheckInterval)
	if !acquired {
		if ctx.Err() == nil && !allFeedsHealthy {
			xlog.Fatal(ctx, "No feed lock acquired within the startup wait limit and some feeds failed startup checks; exiting", zap.Int("eligible_feeds", len(candidates)), zap.Int("configured_feeds", len(config.Feeds)))
		}
		return nil
	}
	feed, locker := selected.Feed, selected.Locker
	lockChannel := locker.LockerContext(ctx)
	// free resources
	for clusterID, connection := range connections {
		if clusterID == feed.Destination || clusterID == feed.Source {
			continue
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), client.DEFAULT_TIMEOUT)
		if err := connection.Client.GetDriver().Close(closeCtx); err != nil {
			xlog.Warn(ctx, "Unable to close unused destination connection", zap.String("cluster_id", clusterID), zap.Error(err))
		}
		closeCancel()
		delete(connections, clusterID)
	}
	source, err := config.ClusterByID(feed.Source)
	if err != nil {
		xlog.Fatal(ctx, "Unable to resolve source cluster", zap.Error(err))
	}
	dstDb := connections[feed.Destination].Client
	xlog.Info(ctx, "Acquired feed lock", zap.String("feed_id", feed.ID))
	lockCtx, ok := <-lockChannel
	if !ok {
		xlog.Info(ctx, "aardappel has been shut down successfully")
		return nil
	}
	srcConnection, exists := connections[feed.Source]
	if !exists {
		srcOpts, err := clusterOptions(lockCtx, source)
		if err != nil {
			xlog.Fatal(ctx, "Unable to configure source cluster authentication", zap.String("feed_id", feed.ID), zap.String("cluster_id", source.ID), zap.Error(err))
		}
		srcDb, err := client.NewYdbClient(lockCtx, logger, source.ConnectionString, srcOpts...)
		if err != nil {
			xlog.Fatal(ctx, "Unable to connect to source cluster", zap.String("feed_id", feed.ID), zap.String("cluster_id", source.ID), zap.Error(err))
		}
		srcConnection = ClusterConnection{Client: srcDb, Options: srcOpts}
		xlog.Debug(ctx, "Connected to source cluster", zap.String("feed_id", feed.ID), zap.String("cluster_id", source.ID))
	}
	srcDb := srcConnection.Client
	if !trySrcConnect(lockCtx, feed, srcDb) {
		xlog.Fatal(ctx, "Source topic checks failed; exiting", zap.String("feed_id", feed.ID), zap.String("cluster_id", source.ID))
	}
	ready = true
	return &AcquiredFeed{
		Feed:        feed,
		Source:      srcDb,
		Destination: dstDb,
		Locker:      locker,
		Context:     lockCtx,
		Cleanup: func() {
			cancel()
			select {
			case <-lockChannel:
			case <-time.After(locker.Ttl):
				xlog.Fatal(ctx, "Timed out waiting for feed lock to be released", zap.String("feed_id", feed.ID), zap.Duration("timeout", locker.Ttl))
			}
		},
	}
}

const defaultLockCheckInterval = 5 * time.Second

func GetLockerRequestBuilder(tableName string) *ydb_locker.LockRequestBuilderImpl {
	return &ydb_locker.LockRequestBuilderImpl{
		TableName:          tableName,
		LockNameColumnName: "id",
		OwnerColumnName:    "lock_owner",
		DeadlineColumnName: "lock_deadline",
	}
}

func createReplicaStateTable(ctx context.Context, client *client.TableClient, stateTable string) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v (id Utf8, step_id Uint64, tx_id Uint64, state Utf8, stage Utf8, "+
		"last_msg Utf8, lock_owner Utf8, lock_deadline Timestamp, PRIMARY KEY(id))", stateTable)
	return client.Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, query, nil)
	})
}

func initReplicaStateTable(ctx context.Context, client *client.TableClient, stateTable string, instanceId string) error {
	param := table.NewQueryParameters(
		table.ValueParam("$instanceId", ydbTypes.UTF8Value(instanceId)),
		table.ValueParam("$state", ydbTypes.UTF8Value(processor.REPLICATION_OK)),
		table.ValueParam("$stage", ydbTypes.UTF8Value(processor.STAGE_INITIAL_SCAN)),
	)
	initQuery := fmt.Sprintf("INSERT INTO %v (id, step_id, tx_id, state, stage) VALUES ($instanceId,0,0, $state, $stage)",
		stateTable)
	err := client.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			_, err := tx.Execute(ctx, initQuery, param)
			return err
		})

	if ydb.IsOperationError(err, Ydb.StatusIds_PRECONDITION_FAILED) {
		return nil
	}
	return err
}

func trySrcConnect(ctx context.Context, feed configInit.Feed, srcDb *client.YdbClient) bool {
	res := DescribeTopics(ctx, feed, srcDb.TopicClient)
	if len(res.TopicPartsCountMap) == len(feed.Streams) {
		xlog.Debug(ctx, "Source topic checks passed", zap.String("feed_id", feed.ID), zap.String("cluster_id", feed.Source))
		return true
	} else {
		return false
	}
}

type ClusterConnection struct {
	Client  *client.YdbClient
	Options []ydb.Option
}

func clusterOptions(ctx context.Context, cluster configInit.Cluster) ([]ydb.Option, error) {
	opts, err := auth.CreateYdbDriverAuthOptions(auth.AuthConfig{
		CredentialsFile:   cluster.OAuthFile,
		StaticToken:       cluster.StaticToken,
		ExchangerEndpoint: cluster.OAuthEndpoint,
	})
	if err != nil {
		return nil, err
	}
	if !cluster.ClientBalancer {
		opts = append(opts, ydb.WithBalancer(balancers.SingleConn()))
	}
	return opts, nil
}

func prepareFeeds(ctx context.Context, logger *zap.Logger, config configInit.Config, owner string) ([]Candidate, map[string]ClusterConnection) {
	clusterIdToStateTables := make(map[string]map[string]struct{})
	for i := range config.Feeds {
		feed := config.Feeds[i]
		tables := clusterIdToStateTables[feed.Destination]
		if tables == nil {
			tables = make(map[string]struct{})
			clusterIdToStateTables[feed.Destination] = tables
		}
		tables[feed.StateTable] = struct{}{}
	}

	connections := make(map[string]ClusterConnection)
	failedConnections := make(map[string]struct{})
	failedTables := make(map[string]map[string]struct{})
	for i := range config.Clusters {
		cluster := config.Clusters[i]
		stateTables, isDestination := clusterIdToStateTables[cluster.ID]
		if !isDestination {
			continue
		}
		opts, err := clusterOptions(ctx, cluster)
		if err != nil {
			xlog.Error(ctx, "Unable to configure destination cluster authentication", zap.String("cluster_id", cluster.ID), zap.Error(err))
			failedConnections[cluster.ID] = struct{}{}
			continue
		}
		db, err := client.NewYdbClient(ctx, logger, cluster.ConnectionString, opts...)
		if err != nil {
			xlog.Error(ctx, "Unable to connect to destination cluster", zap.String("cluster_id", cluster.ID), zap.Error(err))
			failedConnections[cluster.ID] = struct{}{}
			continue
		}
		connections[cluster.ID] = ClusterConnection{Client: db, Options: opts}

		clusterFailedTables := failedTables[cluster.ID]
		if clusterFailedTables == nil {
			clusterFailedTables = make(map[string]struct{})
			failedTables[cluster.ID] = clusterFailedTables
		}
		for stateTable := range stateTables {
			if err := createReplicaStateTable(ctx, db.TableClient, stateTable); err != nil {
				xlog.Error(ctx, "Unable to create replication state table", zap.String("cluster_id", cluster.ID), zap.String("state_table", stateTable), zap.Error(err))
				clusterFailedTables[stateTable] = struct{}{}
			}
		}
	}

	destinationToTargetTables := make(map[string]map[string]struct{})
	candidates := make([]Candidate, 0, len(config.Feeds))
	for i := range config.Feeds {
		feed := config.Feeds[i]
		destinationConnection, connected := connections[feed.Destination]
		if !connected {
			continue
		}
		destinationClient := destinationConnection.Client
		stateTable := feed.StateTable
		if _, tableFailed := failedTables[feed.Destination][stateTable]; tableFailed {
			xlog.Error(ctx, "Skipping feed because its replication state table could not be created", zap.String("feed_id", feed.ID), zap.String("cluster_id", feed.Destination), zap.String("state_table", stateTable))
			continue
		}
		if err := initReplicaStateTable(ctx, destinationClient.TableClient, stateTable, feed.InstanceId); err != nil {
			xlog.Error(ctx, "Unable to initialize replication state; skipping feed", zap.String("feed_id", feed.ID), zap.String("cluster_id", feed.Destination), zap.String("state_table", stateTable), zap.String("instance_id", feed.InstanceId), zap.Error(err))
			continue
		}
		if _, sourceFailed := failedConnections[feed.Source]; sourceFailed {
			xlog.Error(ctx, "Skipping feed because its source cluster connection could not be prepared", zap.String("feed_id", feed.ID), zap.String("cluster_id", feed.Source))
			continue
		}

		targetTables := destinationToTargetTables[feed.Destination]
		if targetTables == nil {
			targetTables = make(map[string]struct{})
			destinationToTargetTables[feed.Destination] = targetTables
		}
		hasMissedTable := false
		for j := range feed.Streams {
			stream := feed.Streams[j]
			if _, checked := targetTables[stream.DstTable]; checked {
				continue
			}
			if _, err := dst_table.DescribeTable(ctx, destinationClient.TableClient, stream.DstTable); err != nil {
				xlog.Error(ctx, "Unable to describe destination table", zap.String("feed_id", feed.ID), zap.String("cluster_id", feed.Destination), zap.String("table", stream.DstTable), zap.Error(err))
				hasMissedTable = true
				continue
			}
			targetTables[stream.DstTable] = struct{}{}
		}
		if hasMissedTable {
			xlog.Error(ctx, "Skipping feed because one or more destination tables could not be described", zap.String("feed_id", feed.ID), zap.String("cluster_id", feed.Destination))
			continue
		}

		storage := &ydb_locker.YdbLockStorage{Db: destinationClient.GetDriver(), ReqBuilder: GetLockerRequestBuilder(stateTable)}
		ttl := time.Duration(feed.MaxExpHbInterval*2) * time.Second
		candidates = append(candidates, Candidate{Feed: feed, Locker: ydb_locker.NewLocker(storage, feed.InstanceId, owner, ttl)})
	}
	return candidates, connections
}

// DescribeTopics loads partition counts and monitoring tags for a feed.
func DescribeTopics(ctx context.Context, feed configInit.Feed, srcDb *client.TopicClient) hb_tracker.TopicPartsCount {
	topics := hb_tracker.NewTopicPartsCount()
	for i := 0; i < len(feed.Streams); i++ {
		cfgStream := &feed.Streams[i]

		desc, err := srcDb.Describe(ctx, cfgStream.SrcTopic)
		if err != nil {
			xlog.Fatal(ctx, "Unable to describe topic",
				zap.String("src_topic", cfgStream.SrcTopic),
				zap.Error(err))
		}

		monTag := misc.TernaryIf(len(cfgStream.MonTag) > 0, cfgStream.MonTag, cfgStream.SrcTopic)

		topics.TopicPartsCountMap[i] = hb_tracker.StreamCfg{PartitionsCount: len(desc.Partitions), MonTag: monTag}
		topics.TotalPartsCount += len(desc.Partitions)
	}

	return *topics
}
