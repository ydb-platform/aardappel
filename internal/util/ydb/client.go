package client

import (
	"aardappel/internal/util/xlog"
	"context"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
	"go.uber.org/zap"
	"strings"
	"time"
)

const DEFAULT_TIMEOUT = 5 * time.Second

func HandleRequestError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		xlog.Error(ctx, "Query timed out", zap.Error(err))
		return fmt.Errorf("query timed out")
	} else {
		return err
	}
}

type TableClient struct {
	client table.Client
}

func (c *TableClient) Do(ctx context.Context, fn func(ctx context.Context, s table.Session) error, opts ...table.Option) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, DEFAULT_TIMEOUT)
	defer cancel()
	err := c.client.Do(ctxWithTimeout, fn, opts...)
	if err != nil {
		return HandleRequestError(ctxWithTimeout, err)
	}
	return nil
}

func (c *TableClient) DoTx(ctx context.Context, fn func(ctx context.Context, tx table.TransactionActor) error) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, DEFAULT_TIMEOUT)
	defer cancel()
	err := c.client.DoTx(ctxWithTimeout, fn)
	if err != nil {
		return HandleRequestError(ctxWithTimeout, err)
	}
	return nil
}

type TopicReader struct {
	reader *topicreader.Reader
}

func (r *TopicReader) Close(ctx context.Context) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, DEFAULT_TIMEOUT)
	defer cancel()
	err := r.reader.Close(ctxWithTimeout)
	if err != nil {
		return HandleRequestError(ctxWithTimeout, err)
	}
	return nil
}

func (r *TopicReader) ReadMessage(ctx context.Context) (*topicreader.Message, error) {
	return r.reader.ReadMessage(ctx)
}

func (r *TopicReader) Commit(ctx context.Context, msg *topicreader.Message) error {
	err := r.reader.Commit(ctx, msg)
	if err != nil {
		return HandleRequestError(ctx, err)
	}
	return nil
}

type TopicWriter struct {
	writer *topicwriter.Writer
}

func (w *TopicWriter) Write(ctx context.Context, msg string) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, DEFAULT_TIMEOUT)
	defer cancel()
	err := w.writer.Write(ctxWithTimeout,
		topicwriter.Message{Data: strings.NewReader(msg)},
	)
	if err != nil {
		return HandleRequestError(ctxWithTimeout, err)
	}
	return nil
}

type TopicClient struct {
	client topic.Client
}

func (c *TopicClient) StartReader(consumer string, path string, opts ...topicoptions.ReaderOption) (*TopicReader, error) {
	reader, err := c.client.StartReader(consumer, topicoptions.ReadTopic(path), opts...)
	if err != nil {
		return nil, err
	}
	var topicReader TopicReader
	topicReader.reader = reader
	return &topicReader, nil
}

func (c *TopicClient) StartWriter(path string) (*TopicWriter, error) {
	writer, err := c.client.StartWriter(path)
	if err != nil {
		return nil, err
	}
	var topicWriter TopicWriter
	topicWriter.writer = writer
	return &topicWriter, nil
}

func (c *TopicClient) Describe(ctx context.Context, path string) (topictypes.TopicDescription, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, DEFAULT_TIMEOUT)
	defer cancel()
	desc, err := c.client.Describe(ctxWithTimeout, path)
	if err != nil {
		return topictypes.TopicDescription{}, HandleRequestError(ctxWithTimeout, err)
	}
	return desc, nil
}

type YdbClient struct {
	driver      *ydb.Driver
	TableClient *TableClient
	TopicClient *TopicClient
}

func NewYdbClient(ctx context.Context, connectionString string, opts ...ydb.Option) (*YdbClient, error) {
	driver, err := ydb.Open(ctx, connectionString, opts...)
	if err != nil {
		return nil, HandleRequestError(ctx, err)
	}
	tableClient := TableClient{client: driver.Table()}
	topicClient := TopicClient{client: driver.Topic()}
	return &YdbClient{driver: driver, TableClient: &tableClient, TopicClient: &topicClient}, nil
}

func (c *YdbClient) GetDriver() *ydb.Driver {
	return c.driver
}
