package main

import (
	configInit "aardappel/internal/config"
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

	var readerId uint8 = 0
	for i := 0; i < len(config.Streams); i++ {
		reader, err := srcDb.Topic().StartReader(config.Streams[i].Consumer, topicoptions.ReadTopic(config.Streams[i].SrcTopic))
		if err != nil {
			xlog.Fatal(ctx, "Unable to create topic reader",
				zap.String("consumer", config.Streams[i].Consumer),
				zap.String("src_topic", config.Streams[i].SrcTopic),
				zap.Error(err))
		}
		xlog.Debug(ctx, "Start reading")
		go topicReader.ReadTopic(ctx, readerId, reader)
		readerId++
	}

	//time.Sleep(20 * time.Second)

	_, dstErr := ydb.Open(ctx, config.DstConnectionString)
	if dstErr != nil {
		xlog.Fatal(ctx, "Unable to connect to dst cluster", zap.Error(dstErr))
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
