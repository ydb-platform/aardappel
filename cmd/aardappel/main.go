package main

import (
	configInit "aardappel/internal/config"
	"aardappel/internal/dst_table"
	feedRuntime "aardappel/internal/feed"
	"aardappel/internal/pmon"
	processor "aardappel/internal/processor"
	topicReader "aardappel/internal/reader"
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	client "aardappel/internal/util/ydb"
	"aardappel/internal/util/ydb/ydb_locker"
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

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"go.uber.org/zap"
)

func EstimateReplicationLagSec(pos types.Position) float32 {
	ts := time.Now().UTC()
	return float32(max(uint64(ts.UnixMilli()), pos.Step)-pos.Step) / 1000.0
}

func DoReplication(ctx context.Context, prc *processor.Processor, dstTables []*dst_table.DstTable,
	lockExecutor func(fn func(context.Context, table.Session, table.Transaction) error) error, mon pmon.Metrics) error {
	passed := time.Now().UnixMilli()
	stats, err := prc.DoReplication(ctx, dstTables, lockExecutor)
	if err != nil {
		errMsg := fmt.Sprintf("Unable to perform replication without error")
		return types.ReturnError(ctx, err, errMsg)
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
	xlog.Debug(ctx, "Replication step ok", zap.Int("modifications", stats.ModificationsCount),
		zap.Float32("mps", perSecond),
		zap.Uint64("last quorum HB step", stats.LastHeartBeat.Step),
		zap.Uint64("last quorum HB tx_id", stats.LastHeartBeat.TxId),
		zap.Float32("commit duration", float32(stats.CommitDurationMs)/1000),
		zap.Int("request size", stats.RequestSize),
		zap.Float32("quorum waiting duration", float32(stats.QuorumWaitingDurationMs)/1000),
		zap.Float32("replication lag estimation:", lag))

	return nil
}

func doMain(ctx context.Context, feed configInit.Feed, srcDb *client.TopicClient, dstDb *client.YdbClient,
	locker *ydb_locker.Locker, mon pmon.Metrics, errChannel chan error) error {
	topics := feedRuntime.DescribeTopics(ctx, feed, srcDb)

	xlog.Debug(ctx, "All topics described",
		zap.Int("total parts", topics.TotalPartsCount))

	var conflictHandler processor.ConflictHandler

	if feed.CmdQueue != nil {
		xlog.Debug(ctx, "Command queue present in feed",
			zap.String("path", feed.CmdQueue.Path),
			zap.String("consumer", feed.CmdQueue.Consumer))
		conflictHandler = processor.NewCmdQueueConflictHandler(
			ctx, feed.InstanceId, feed.CmdQueue.Path, feed.CmdQueue.Consumer, dstDb.TopicClient)
	}

	var dlQueue *processor.DLQueue
	if feed.DLQueue != nil {
		xlog.Info(ctx, "Dead letter queue present in feed",
			zap.String("path", feed.DLQueue.Path))
		topicWriter, err := dstDb.TopicClient.StartWriter(feed.DLQueue.Path)
		if err != nil {
			xlog.Fatal(ctx, "Unable to start writer for dlq", zap.Error(err))
		}
		dlQueue = processor.NewDlQueue(ctx, topicWriter)
	}

	prc, err := processor.NewProcessor(ctx, topics, feed.StateTable, dstDb.TableClient, feed.InstanceId, feed.KeyFilter, feed.CommitOffsetMode, feed.MaxConcurrentOffsetCommits, feed.LegacyReaderScheduling, feed.LegacyInitialScan)
	if err != nil {
		xlog.Fatal(ctx, "Unable to create processor", zap.Error(err))
	}

	if feed.MaxExpHbInterval != 0 {
		xlog.Info(ctx, "start heartbeat tracker guard timer", zap.Uint32("timeout in seconds", feed.MaxExpHbInterval))
		if reflect.ValueOf(mon).IsNil() {
			prc.StartHbGuard(ctx, feed.MaxExpHbInterval, nil)
		} else {
			prc.StartHbGuard(ctx, feed.MaxExpHbInterval, mon)
		}
	}

	var dstTables []*dst_table.DstTable

	for i := 0; i < len(feed.Streams); i++ {
		cfgStream := &feed.Streams[i]
		var readerOptions []topicoptions.ReaderOption
		var updateCb topicReader.UpdateOffsetFunc
		if !feed.CommitOffsetMode {
			var startCb topicoptions.GetPartitionStartOffsetFunc
			startCb, updateCb = topicReader.MakeTopicReaderGuard(errChannel)
			readerOptions = append(readerOptions, topicoptions.WithReaderGetPartitionStartOffset(startCb))
		}
		reader, err := srcDb.StartReader(cfgStream.Consumer, cfgStream.SrcTopic, readerOptions...)

		if err != nil {
			xlog.Fatal(ctx, "Unable to create topic reader",
				zap.String("consumer", cfgStream.Consumer),
				zap.String("src_topic", cfgStream.SrcTopic),
				zap.Error(err))
		}
		if feed.CommitOffsetMode {
			prc.RegisterTopicReader(uint32(i), reader)
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
		go topicReader.ReadTopic(ctx, streamInfo, reader, prc, conflictHandler, updateCb, dlQueue, errChannel, feed.CommitOffsetMode)
	}

	lockExecutor := func(fn func(context.Context, table.Session, table.Transaction) error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return locker.ExecuteUnderLock(ctx, fn)
	}

	for ctx.Err() == nil {
		err := DoReplication(ctx, prc, dstTables, lockExecutor, mon)
		if err != nil {
			errMsg := fmt.Sprintf("doMain")
			return types.ReturnError(ctx, err, errMsg)
		}
	}

	return nil
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

	errChannel := make(chan error)

	go func() {
		for {
			select {
			case sig := <-signalChannel:
				xlog.Info(ctx, "Got OS signal, stopping aardappel....", zap.String("signal name", sig.String()))
				cancel()
			case err := <-errChannel:
				if err == nil {
					continue
				}
				var appErr *types.Error
				if errors.As(err, &appErr) {
					switch appErr.Kind() {
					case types.Graceful:
						xlog.Info(ctx, "Got error, stopping aardappel....", zap.String("error", appErr.Error()))
						cancel()
					case types.Fatal:
						xlog.Fatal(ctx, "Got error, stopping aardappel....", zap.String("error", appErr.Error()))
					}
				} else {
					xlog.Error(ctx, "Unexpected error", zap.Error(err))
				}
			}
		}
	}()

	defer logger.Sync()

	confStr, err := config.ToString()
	if err == nil {
		xlog.Debug(ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr))
	}

	var monServer *pmon.PromServer
	if config.MonServer != nil {
		monServer = pmon.NewPromServer(ctx, config.MonServer)
		defer monServer.Stop()
		monServer.SetCompleted()
	}

	acquired := feedRuntime.AcquireFeed(ctx, logger, config)
	if acquired == nil {
		return
	}
	feed := acquired.Feed
	var mon *pmon.PromMon
	if monServer != nil {
		mon = monServer.NewFeed(feed.ID)
	}

	err = doMain(acquired.Context, feed, acquired.Source.TopicClient, acquired.Destination, acquired.Locker, mon, errChannel)

	if err != nil {
		xlog.Error(ctx, "Replication stopped with an error", zap.String("feed_id", feed.ID), zap.Error(err))
	} else {
		xlog.Info(ctx, "Replication stopped", zap.String("feed_id", feed.ID))
	}
	cancel()
	acquired.Cleanup()
	xlog.Info(ctx, "aardappel has been shut down successfully")
}
