package main

import (
	"aardappel/internal/protos"
	"aardappel/internal/util/xlog"
	"context"
	"flag"
	"google.golang.org/protobuf/encoding/prototext"
	"os"
)

func main() {
	var confPath string
	var config protos.Config

	flag.StringVar(&confPath, "config", "aardappel.conf", "aardappel configuration file")
	flag.Parse()

	ctx := context.Background()

	// Setup logging
	logger := xlog.SetupLogging(true)
	xlog.SetInternalLogger(logger)
	defer logger.Sync()

	if len(confPath) != 0 {
		confTxt, err := os.ReadFile(confPath)
		if err != nil {
			xlog.Error(ctx, "Unable to read configuration file: "+confPath+", err: "+err.Error())
			os.Exit(1)
		}
		err = prototext.Unmarshal(confTxt, &config)
		if err != nil {
			xlog.Error(ctx, "Unable to parse configuration file: "+confPath+", err: "+err.Error())
			os.Exit(1)
		}
	}

	xlog.Debug(ctx, "Use configuration file: "+confPath+", config:\n"+config.String())
	// Connect to YDB
	//db, err := ydb.Open(ctx, sugar.DSN(endpoint, database, false))
	//if err != nil {
	//	logger.Fatal("db connection error", zap.Error(err))
	//	return
	//}
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
