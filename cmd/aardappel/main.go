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
	"time"
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

	var totalPartitions int
	for i := 0; i < len(config.Streams); i++ {
		desc, err := srcDb.Topic().Describe(ctx, config.Streams[i].SrcTopic)
		if err != nil {
			xlog.Fatal(ctx, "Unable to describe topic",
				zap.String("src_topic", config.Streams[i].SrcTopic),
				zap.Error(err))
		}
		totalPartitions += len(desc.Partitions)
	}

	xlog.Debug(ctx, "All topics described",
		zap.Int("total parts", totalPartitions))

	var readerId uint8 = 0
	prc := processor.NewProcessor(totalPartitions)
	for i := 0; i < len(config.Streams); i++ {
		reader, err := srcDb.Topic().StartReader(config.Streams[i].Consumer, topicoptions.ReadTopic(config.Streams[i].SrcTopic))
		if err != nil {
			xlog.Fatal(ctx, "Unable to create topic reader",
				zap.String("consumer", config.Streams[i].Consumer),
				zap.String("src_topic", config.Streams[i].SrcTopic),
				zap.Error(err))
		}
		xlog.Debug(ctx, "Start reading")
		go topicReader.ReadTopic(ctx, readerId, reader, prc)
		readerId++
	}

	for {
		_, err = prc.FormatTx(ctx)
		if err != nil {
			xlog.Fatal(ctx, "Unable to format tx for destination")
		}
	}

	time.Sleep(20 * time.Second)

	db, err := ydb.Open(ctx, config.DstConnectionString)
	if err != nil {
		xlog.Fatal(ctx, "Unable to connect to dst cluster", zap.Error(err))
	}

	client := db.Table()
	for i := 0; i < len(config.Streams); i++ {
		dstTable := dst_table.NewDstTable(client, config.Streams[i].DstTable)
		err := dstTable.Init(ctx)
		if err != nil {
			xlog.Fatal(ctx, "Unable to init dst table", zap.Error(err))
		}
	}

	//client := db.Table()

	// Perform YDB operation
	//err = ydb_operations.SomeYdbOperation(ctx, client)
	//if err != nil {
	//	xlog.Error(ctx, "ydb operation error", zap.Error(err))
	//	return
	//}

	// Create and print a protobuf message
	//var x protos.SomeMessage
	//x.Port = 123
	//s, err := prototext.Marshal(&x)
	//if err != nil {
	//	xlog.Error(ctx, "protobuf marshal error", zap.Error(err))
	//	return
	//}
	//xlog.Info(ctx, "protobuf message", zap.String("message", string(s))
}
