package main

import (
	configInit "aardappel/internal/config"
	"aardappel/internal/dst_table"
	processor "aardappel/internal/processor"
	topicReader "aardappel/internal/reader"
	"aardappel/internal/util/xlog"
	"context"
	"flag"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"go.uber.org/zap"
)

func main() {
	var confPath string

	flag.StringVar(&confPath, "config", "config.yaml", "aardappel configuration file")
	flag.Parse()

	ctx := context.Background()

	// Setup logging
	logger := xlog.SetupLogging(true)
	xlog.SetInternalLogger(logger)
	defer logger.Sync()

	// Setup config
	config, err := configInit.InitConfig(ctx, confPath)
	if err != nil {
		xlog.Fatal(ctx, "Unable to initialize config", zap.Error(err))
	}
	confStr, err := config.ToString()
	if err == nil {
		xlog.Debug(ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr))
	}

	opts := []ydb.Option{
		ydb.WithAccessTokenCredentials("root@builtin"),
	}

	// Connect to YDB
	srcDb, err := ydb.Open(ctx, config.SrcConnectionString, opts...)
	if err != nil {
		xlog.Fatal(ctx, "Unable to connect to src cluster", zap.Error(err))
	}
	xlog.Debug(ctx, "YDB opened")

	dstDb, err := ydb.Open(ctx, config.DstConnectionString, opts...)
	if err != nil {
		xlog.Fatal(ctx, "Unable to connect to dst cluster", zap.Error(err))
	}

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

	prc, err := processor.NewProcessor(ctx, totalPartitions, config.StateTable, dstDb.Table())
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

	for {
		err := prc.DoReplication(ctx, dstTables, dstDb.Table())
		if err != nil {
			xlog.Fatal(ctx, "Unable to perform replication without error")
		}
	}
}
