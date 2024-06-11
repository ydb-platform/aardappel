package main

import (
	"aardappel/internal/protos"
	"aardappel/internal/util/xlog"
	"aardappel/internal/ydb_operations"
	"context"
	"flag"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/prototext"
)

func main() {
	var endpoint string
	var database string

	flag.StringVar(&endpoint, "endpoint", "localhost:2136", "YDB endpoint")
	flag.StringVar(&database, "database", "local", "YDB database")
	flag.Parse()

	ctx := context.Background()

	// Setup logging
	logger := xlog.SetupLogging(true)
	xlog.SetInternalLogger(logger)
	defer logger.Sync()

	// Connect to YDB
	db, err := ydb.Open(ctx, sugar.DSN(endpoint, database, false))
	if err != nil {
		logger.Fatal("db connection error", zap.Error(err))
		return
	}
	client := db.Table()

	// Perform YDB operation
	err = ydb_operations.SomeYdbOperation(ctx, client)
	if err != nil {
		xlog.Error(ctx, "ydb operation error", zap.Error(err))
		return
	}

	// Create and print a protobuf message
	var x protos.SomeMessage
	x.Port = 123
	s, err := prototext.Marshal(&x)
	if err != nil {
		xlog.Error(ctx, "protobuf marshal error", zap.Error(err))
		return
	}
	xlog.Info(ctx, "protobuf message", zap.String("message", string(s)))

}
