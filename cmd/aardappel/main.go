package main

import (
	configInit "aardappel/internal/config"
	"aardappel/internal/dst_table"
	processor "aardappel/internal/processor"
	topicReader "aardappel/internal/reader"
	"aardappel/internal/util/xlog"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/robdrynkin/ydb_locker/pkg/ydb_locker"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"go.uber.org/zap"
	"log"
	"os"
	"time"
)

func createYdbDriverAuthOptions(oauthFile string, staticToken string) ([]ydb.Option, error) {
	if (len(oauthFile) > 0 && len(staticToken) > 0) || (len(oauthFile) == 0 && len(staticToken) == 0) {
		return nil, errors.New("it's either oauth2_file or static_token option must be set")
	}

	if len(oauthFile) > 0 {
		return []ydb.Option{
			ydb.WithOauth2TokenExchangeCredentialsFile(oauthFile),
		}, nil
	}

	if len(staticToken) > 0 {
		return []ydb.Option{
			ydb.WithAccessTokenCredentials(staticToken),
		}, nil
	}
	return nil, errors.New("not supported")
}

func GetLockerRequestBuilder(tableName string) *ydb_locker.LockRequestBuilderImpl {
	return &ydb_locker.LockRequestBuilderImpl{
		TableName:          tableName,
		LockNameColumnName: "id",
		OwnerColumnName:    "lock_owner",
		DeadlineColumnName: "lock_deadline",
	}
}

func DoReplication(ctx context.Context, prc *processor.Processor, dstTables []*dst_table.DstTable,
	lockExecutor func(fn func(context.Context, table.Session, table.Transaction) error) error) {
	passed := time.Now().UnixMilli()
	stats, err := prc.DoReplication(ctx, dstTables, lockExecutor)
	if err != nil {
		xlog.Fatal(ctx, "Unable to perform replication without error", zap.Error(err))
	}
	passed = time.Now().UnixMilli() - passed
	perSecond := float32(stats.ModificationsCount) / (float32(passed) / 1000.0)
	xlog.Info(ctx, "Replication step ok", zap.Int("modifications", stats.ModificationsCount),
		zap.Float32("mps", perSecond),
		zap.Uint64("last quorum HB", stats.LastHeartBeat),
		zap.Float32("commit duration", float32(stats.CommitDurationMs)/1000),
		zap.Float32("waitForQuorumDuration", float32(passed-stats.CommitDurationMs)/1000))
}

func doMain(ctx context.Context, config configInit.Config, srcDb *ydb.Driver, dstDb *ydb.Driver, locker *ydb_locker.Locker) {
	var totalPartitions int
	var streamDbgInfos []string
	for i := 0; i < len(config.Streams); i++ {
		desc, err := srcDb.Topic().Describe(ctx, config.Streams[i].SrcTopic)
		if err != nil {
			xlog.Fatal(ctx, "Unable to describe topic",
				zap.String("src_topic", config.Streams[i].SrcTopic),
				zap.Error(err))
		}
		totalPartitions += len(desc.Partitions)
		streamDbgInfos = append(streamDbgInfos, desc.Path)
	}

	xlog.Debug(ctx, "All topics described",
		zap.Int("total parts", totalPartitions))

	prc, err := processor.NewProcessor(ctx, totalPartitions, config.StateTable, dstDb.Table(), config.InstanceId)
	if err != nil {
		xlog.Fatal(ctx, "Unable to create processor", zap.Error(err))
	}

	if config.MaxExpHbInterval != 0 {
		xlog.Info(ctx, "start heartbeat tracker guard timer", zap.Uint32("timeout in seconds", config.MaxExpHbInterval))
		prc.StartHbGuard(ctx, config.MaxExpHbInterval, streamDbgInfos)
	}
	var dstTables []*dst_table.DstTable
	for i := 0; i < len(config.Streams); i++ {
		reader, err := srcDb.Topic().StartReader(config.Streams[i].Consumer, topicoptions.ReadTopic(config.Streams[i].SrcTopic))
		if err != nil {
			xlog.Fatal(ctx, "Unable to create topic reader",
				zap.String("consumer", config.Streams[i].Consumer),
				zap.String("src_topic", config.Streams[i].SrcTopic),
				zap.Error(err))
		}
		dstTables = append(dstTables, dst_table.NewDstTable(dstDb.Table(), config.Streams[i].DstTable))
		err = dstTables[i].Init(ctx)
		if err != nil {
			xlog.Fatal(ctx, "Unable to init dst table")
		}
		xlog.Debug(ctx, "Start reading")
		go topicReader.ReadTopic(ctx, uint32(i), reader, prc)
	}

	lockExecutor := func(fn func(context.Context, table.Session, table.Transaction) error) error {
		return locker.ExecuteUnderLock(ctx, fn)
	}

	for ctx.Err() == nil {
		DoReplication(ctx, prc, dstTables, lockExecutor)
	}
}

func main() {
	var confPath string

	flag.StringVar(&confPath, "config", "config.yaml", "aardappel configuration file")
	flag.Parse()

	ctx := context.Background()

	// Setup config
	config, err := configInit.InitConfig(ctx, confPath)
	if err != nil {
		log.Fatal(ctx, "Unable to initialize config: ", err)
	}

	// Setup logging
	logger := xlog.SetupLogging(config.LogLevel)
	xlog.SetInternalLogger(logger)
	defer logger.Sync()

	confStr, err := config.ToString()
	if err == nil {
		xlog.Debug(ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr))
	}

	srcOpts, err := createYdbDriverAuthOptions(config.SrcOAuthFile, config.SrcStaticToken)
	if err != nil {
		xlog.Fatal(ctx, "Unable to create auth option for src",
			zap.Error(err))
	}

	dstOpts, err := createYdbDriverAuthOptions(config.DstOAuthFile, config.DstStaticToken)
	if err != nil {
		xlog.Fatal(ctx, "Unable to create auth option for dst",
			zap.Error(err))
	}

	if config.SrcClientBalancer == false {
		srcOpts = append(srcOpts, ydb.WithBalancer(balancers.SingleConn()))
	}

	if config.DstClientBalancer == false {
		dstOpts = append(dstOpts, ydb.WithBalancer(balancers.SingleConn()))
	}

	// Connect to YDB
	srcDb, err := ydb.Open(ctx, config.SrcConnectionString, srcOpts...)
	if err != nil {
		xlog.Fatal(ctx, "Unable to connect to src cluster", zap.Error(err))
	}
	xlog.Debug(ctx, "YDB src opened")

	dstDb, err := ydb.Open(ctx, config.DstConnectionString, dstOpts...)
	if err != nil {
		xlog.Fatal(ctx, "Unable to connect to dst cluster", zap.Error(err))
	}
	xlog.Debug(ctx, "YDB dst opened")

	hostname, _ := os.Hostname()
	owner := fmt.Sprintf("lock_%s_%s", hostname, uuid.New().String())

	reqBuilder := GetLockerRequestBuilder(config.StateTable)
	lockStorage := ydb_locker.YdbLockStorage{Db: dstDb, ReqBuilder: reqBuilder}
	locker := ydb_locker.NewLocker(&lockStorage,
		config.InstanceId, owner,
		time.Duration(config.MaxExpHbInterval*2)*time.Second)

	lockChannel := locker.LockerContext(ctx)
	for {
		select {
		case lockCtx := <-lockChannel:
			{
				doMain(lockCtx, config, srcDb, dstDb, locker)
			}
		case <-time.After(5 * time.Second):
			xlog.Info(ctx, "unable to get lock, other instance of aardappel is running")
		}
	}
}
