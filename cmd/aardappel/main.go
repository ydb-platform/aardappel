package main

import (
	configInit "aardappel/internal/config"
	"aardappel/internal/dst_table"
	"aardappel/internal/hb_tracker"
	"aardappel/internal/pmon"
	processor "aardappel/internal/processor"
	topicReader "aardappel/internal/reader"
	"aardappel/internal/types"
	"aardappel/internal/util/misc"
	"aardappel/internal/util/xlog"
	client "aardappel/internal/util/ydb"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/robdrynkin/ydb_locker/pkg/ydb_locker"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"go.uber.org/zap"
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

func EstimateReplicationLagSec(pos types.Position) float32 {
	ts := time.Now().UTC()
	return float32(uint64(ts.UnixMilli())-pos.Step) / 1000.0
}

func DoReplication(ctx context.Context, prc *processor.Processor, dstTables []*dst_table.DstTable,
	lockExecutor func(fn func(context.Context, table.Session, table.Transaction) error) error, mon pmon.Metrics) {
	passed := time.Now().UnixMilli()
	stats, err := prc.DoReplication(ctx, dstTables, lockExecutor)
	if err != nil {
		if ctx.Err() != nil {
			xlog.Error(ctx, "Context cancelled or expired during replication step", zap.Error(err))
			return
		}
		xlog.Fatal(ctx, "Unable to perform replication without error", zap.Error(err))
	}
	passed = time.Now().UnixMilli() - passed
	perSecond := float32(stats.ModificationsCount) / (float32(passed) / 1000.0)
	lag := EstimateReplicationLagSec(stats.LastHeartBeat)
	if !reflect.ValueOf(mon).IsNil() {
		mon.ModificationCount(stats.ModificationsCount)
		mon.CommitDuration(float64(stats.CommitDurationMs) / 1000)
		mon.RequestSize(stats.RequestSize)
		mon.QuorumWaitingDuration(float64(stats.QuorumWaitingDurationMs) / 1000)
		var memStat runtime.MemStats
		runtime.ReadMemStats(&memStat)
		mon.HeapAllocated(memStat.HeapAlloc)
		mon.ReplicationLagEst(lag)
		for i := 0; i < len(stats.PerTableStats); i++ {
			monTag := dstTables[i].MonTag
			mon.ModificationCountFromTopic(stats.PerTableStats[i].ModificationsCount, monTag)
		}
	}
	xlog.Info(ctx, "Replication step ok", zap.Int("modifications", stats.ModificationsCount),
		zap.Float32("mps", perSecond),
		zap.Uint64("last quorum HB step", stats.LastHeartBeat.Step),
		zap.Uint64("last quorum HB tx_id", stats.LastHeartBeat.TxId),
		zap.Float32("commit duration", float32(stats.CommitDurationMs)/1000),
		zap.Int("request size", stats.RequestSize),
		zap.Float32("quorum waiting duration", float32(stats.QuorumWaitingDurationMs)/1000),
		zap.Float32("replication lag estimation:", lag))
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

func doDescribeTopics(ctx context.Context, config configInit.Config, srcDb *client.TopicClient) hb_tracker.TopicPartsCount {
	topics := hb_tracker.NewTopicPartsCount()
	for i := 0; i < len(config.Streams); i++ {
		cfgStream := &config.Streams[i]

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

func doMain(ctx context.Context, config configInit.Config, srcDb *client.TopicClient, dstDb *client.YdbClient,
	locker *ydb_locker.Locker, mon pmon.Metrics) {
	topics := doDescribeTopics(ctx, config, srcDb)

	xlog.Debug(ctx, "All topics described",
		zap.Int("total parts", topics.TotalPartsCount))

	var conflictHandler processor.ConflictHandler

	if config.CmdQueue != nil {
		xlog.Debug(ctx, "Command queue present in config",
			zap.String("path", config.CmdQueue.Path),
			zap.String("consumer", config.CmdQueue.Consumer))
		conflictHandler = processor.NewCmdQueueConflictHandler(
			ctx, config.InstanceId, config.CmdQueue.Path, config.CmdQueue.Consumer, dstDb.TopicClient)
	}

	var dlQueue *processor.DLQueue
	if config.DLQueue != nil {
		xlog.Info(ctx, "Dead letter queue present in config",
			zap.String("path", config.DLQueue.Path))
		topicWriter, err := dstDb.TopicClient.StartWriter(config.DLQueue.Path)
		if err != nil {
			xlog.Fatal(ctx, "Unable to start writer for dlq", zap.Error(err))
		}
		dlQueue = processor.NewDlQueue(ctx, topicWriter)
	}

	prc, err := processor.NewProcessor(ctx, topics, config.StateTable, dstDb.TableClient, config.InstanceId, config.KeyFilter)
	if err != nil {
		xlog.Fatal(ctx, "Unable to create processor", zap.Error(err))
	}

	if config.MaxExpHbInterval != 0 {
		xlog.Info(ctx, "start heartbeat tracker guard timer", zap.Uint32("timeout in seconds", config.MaxExpHbInterval))
		if reflect.ValueOf(mon).IsNil() {
			prc.StartHbGuard(ctx, config.MaxExpHbInterval, nil)
		} else {
			prc.StartHbGuard(ctx, config.MaxExpHbInterval, mon)
		}
	}

	var dstTables []*dst_table.DstTable

	for i := 0; i < len(config.Streams); i++ {
		cfgStream := &config.Streams[i]
		startCb, updateCb := topicReader.MakeTopicReaderGuard()
		reader, err := srcDb.StartReader(config.Streams[i].Consumer, cfgStream.SrcTopic,
			topicoptions.WithReaderGetPartitionStartOffset(startCb))

		if err != nil {
			xlog.Fatal(ctx, "Unable to create topic reader",
				zap.String("consumer", cfgStream.Consumer),
				zap.String("src_topic", cfgStream.SrcTopic),
				zap.Error(err))
		}
		dstTables = append(dstTables,
			dst_table.NewDstTable(dstDb.TableClient, cfgStream.DstTable, topics.TopicPartsCountMap[i].MonTag))

		err = dstTables[i].Init(ctx)
		if err != nil {
			xlog.Fatal(ctx, "Unable to init dst table")
		}

		streamInfo := topicReader.StreamInfo{
			Id:              uint32(i),
			TopicPath:       cfgStream.SrcTopic,
			PartCount:       topics.TopicPartsCountMap[i].PartitionsCount,
			ProblemStrategy: cfgStream.ProblemStrategy}
		xlog.Debug(ctx, "Start reading")
		go topicReader.ReadTopic(ctx, streamInfo, reader, prc, conflictHandler, updateCb, dlQueue)
	}

	lockExecutor := func(fn func(context.Context, table.Session, table.Transaction) error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return locker.ExecuteUnderLock(ctx, fn)
	}

	for ctx.Err() == nil {
		DoReplication(ctx, prc, dstTables, lockExecutor, mon)
	}
}

func trySrcConnect(ctx context.Context, config configInit.Config, srcOpts []ydb.Option) bool {
	srcDb, err := client.NewYdbClient(ctx, config.SrcConnectionString, srcOpts...)
	if err != nil {
		xlog.Fatal(ctx, "Unable to connect to src cluster", zap.Error(err))
	}
	res := doDescribeTopics(ctx, config, srcDb.TopicClient)
	if len(res.TopicPartsCountMap) == len(config.Streams) {
		xlog.Debug(ctx, "Initial check for YDB src connection passed")
		return true
	} else {
		return false
	}
}

func tryDstConnect(ctx context.Context, config configInit.Config, tableClient *client.TableClient) bool {
	for i := 0; i < len(config.Streams); i++ {
		cfgStream := &config.Streams[i]
		_, err := dst_table.DescribeTable(ctx, tableClient, cfgStream.DstTable)
		if err != nil {
			return false
		}
	}
	return true
}

func main() {
	var confPath string

	flag.StringVar(&confPath, "config", "config.yaml", "aardappel configuration file")
	flag.Parse()

	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer func() {
		signal.Stop(signalChannel)
		cancel()
	}()

	// Setup config
	config, err := configInit.InitConfig(ctx, confPath)
	if err != nil {
		log.Fatal(ctx, "Unable to initialize config: ", err)
	}

	// Setup logging
	logger := xlog.SetupLogging(config.LogLevel)
	xlog.SetInternalLogger(logger)

	go func() {
		select {
		case sig := <-signalChannel:
			xlog.Info(ctx, "Got OS signal, stopping aardappel....", zap.String("signal name", sig.String()))
			cancel()
		}
	}()

	defer logger.Sync()

	confStr, err := config.ToString()
	if err == nil {
		xlog.Debug(ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr))
	}

	var mon *pmon.PromMon
	if config.MonServer != nil {
		mon = pmon.NewPromMon(ctx, config.MonServer)
		defer mon.Stop()
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

	dstDb, err := client.NewYdbClient(ctx, config.DstConnectionString, dstOpts...)
	if err != nil {
		xlog.Fatal(ctx, "Unable to connect to dst cluster", zap.Error(err))
	}
	xlog.Debug(ctx, "YDB dst opened")

	err = createReplicaStateTable(ctx, dstDb.TableClient, config.StateTable)
	if err != nil {
		xlog.Fatal(ctx, "Replication startup failed",
			zap.String("unable to create table", config.StateTable), zap.Error(err))
	}

	err = initReplicaStateTable(ctx, dstDb.TableClient, config.StateTable, config.InstanceId)
	if err != nil {
		xlog.Fatal(ctx, "Replication startup failed",
			zap.String("unable to init table", config.StateTable), zap.Error(err))
	}

	if trySrcConnect(ctx, config, srcOpts) {
		if tryDstConnect(ctx, config, dstDb.TableClient) {
			if mon != nil {
				mon.SetCompleted()
			}
		} else {
			xlog.Fatal(ctx, "Unable to connect to DST cluster before lock stage")
		}
	} else {
		xlog.Fatal(ctx, "Unable to connect to SRC cluster before lock stage")
	}

	hostname, _ := os.Hostname()
	owner := fmt.Sprintf("lock_%s_%s", hostname, uuid.New().String())

	reqBuilder := GetLockerRequestBuilder(config.StateTable)
	lockStorage := ydb_locker.YdbLockStorage{Db: dstDb.GetDriver(), ReqBuilder: reqBuilder}
	lockTtl := time.Duration(config.MaxExpHbInterval*2) * time.Second
	locker := ydb_locker.NewLocker(&lockStorage, config.InstanceId, owner, lockTtl)

	lockChannel := locker.LockerContext(ctx)
	cont := true
	var lockErrCnt uint32
	for cont {
		select {
		case lockCtx, ok := <-lockChannel:
			// Connect to YDB
			if ok != true {
				cont = false
				continue
			}
			lockErrCnt = 0
			srcDb, err := client.NewYdbClient(lockCtx, config.SrcConnectionString, srcOpts...)
			if err != nil {
				xlog.Fatal(ctx, "Unable to connect to src cluster", zap.Error(err))
			}
			xlog.Debug(ctx, "YDB src opened")
			doMain(lockCtx, config, srcDb.TopicClient, dstDb, locker, mon)
			cont = false
			select {
			case _, ok := <-lockChannel:
				if !ok {
					continue
				}
			case <-time.After(lockTtl):
				xlog.Fatal(ctx, "Timeout waiting for lock to release")
			}
		case <-time.After(5 * time.Second):
			if lockErrCnt == 10 {
				cont = false
				continue
			} else {
				lockErrCnt++
			}
			xlog.Info(ctx, "unable to get lock, other instance of aardappel is running")
		}
	}

	if lockErrCnt != 0 {
		xlog.Error(ctx, "aardappel has been shutted down after multiple getting lock errors")
	} else {
		xlog.Info(ctx, "aardappel has been shutted down successfully ")
	}
}
