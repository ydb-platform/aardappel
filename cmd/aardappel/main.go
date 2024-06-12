package main

import (
	configInit "aardappel/internal/config"
	"aardappel/internal/util/xlog"
	"context"
	"flag"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
	"os"
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
		xlog.Error(ctx, "Unable to initialize config", zap.Error(err))
		os.Exit(1)
	}
	confStr, err := config.ToString()
	if err == nil {
		xlog.Debug(ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr))
	}

	// Connect to YDB
	_, srcErr := ydb.Open(ctx, config.SrcConnectionString)
	if srcErr != nil {
		xlog.Error(ctx, "Unable to connect to src cluster", zap.Error(srcErr))
		os.Exit(1)
	}

	_, dstErr := ydb.Open(ctx, config.DstConnectionString)
	if dstErr != nil {
		xlog.Error(ctx, "Unable to connect to dst cluster", zap.Error(dstErr))
		os.Exit(1)
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
