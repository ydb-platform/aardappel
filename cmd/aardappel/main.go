package main

import (
	configInit "aardappel/internal/config"
	"aardappel/internal/dst_table"
	processor "aardappel/internal/processor"
	"aardappel/internal/pusher"
	topicReader "aardappel/internal/reader"
	"aardappel/internal/types"
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

	prc, err := processor.NewProcessor(ctx, totalPartitions, config.StateTable, dstDb.Table())
	if err != nil {
		xlog.Fatal(ctx, "Unable to create processor", zap.Error(err))
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
		txDataPerTable := make([][]types.TxData, len(dstTables))
		batch, err := prc.FormatTx(ctx)
		if err != nil {
			xlog.Fatal(ctx, "Unable to format tx for destination")
		}
		for i := 0; i < len(batch.TxData); i++ {
			txDataPerTable[batch.TxData[i].TableId] = append(txDataPerTable[batch.TxData[i].TableId], batch.TxData[i])
		}
		if len(txDataPerTable) != len(dstTables) {
			xlog.Fatal(ctx, "Size of dstTables and tables in the tx mismatched",
				zap.Int("txDataPertabe", len(txDataPerTable)),
				zap.Int("dstTable", len(dstTables)))
		}
		var query dst_table.PushQuery
		for i := 0; i < len(txDataPerTable); i++ {
			q, err := dstTables[i].GenQuery(ctx, txDataPerTable[i], i)
			if err != nil {
				xlog.Fatal(ctx, "Unable to generate query")
			}
			query.Query += q.Query
			query.Parameters = append(query.Parameters, q.Parameters...)

		}
		xlog.Debug(ctx, "Query to perform", zap.String("query", query.Query))
		err = pusher.PushAsSingleTx(ctx, dstDb.Table(), query, types.Position{Step: batch.Hb.Step, TxId: 0}, config.StateTable)
		if err != nil {
			xlog.Fatal(ctx, "Unable to push tx", zap.Error(err))
		}
		for i := 0; i < len(batch.TxData); i++ {
			err := batch.TxData[i].CommitTopic()
			if err != nil {
				xlog.Fatal(ctx, "Unable to commit topic fot dataTx", zap.Error(err))
			}
		}
		xlog.Debug(ctx, "commit hb in topic")
		err = batch.Hb.CommitTopic()
		if err != nil {
			xlog.Fatal(ctx, "Unable to commit topic fot Hb", zap.Error(err))
		}
	}
}
