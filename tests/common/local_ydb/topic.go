package local_ydb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// CreateTopic creates a raw topic with a consumer.
func CreateTopic(t testing.TB, ctx context.Context, driver *ydb.Driver, topic, consumer string, partitionCount int64) {
	t.Helper()
	err := driver.Topic().Create(ctx, topic,
		topicoptions.CreateWithMinActivePartitions(partitionCount),
		topicoptions.CreateWithConsumer(topictypes.Consumer{
			Name:            consumer,
			SupportedCodecs: []topictypes.Codec{topictypes.CodecRaw},
		}),
	)
	require.NoError(t, err)
}

// WriteTopicMessages writes messages synchronously to the selected partition.
func WriteTopicMessages(t testing.TB, ctx context.Context, driver *ydb.Driver, topic string, partitionID int64, messages ...string) {
	t.Helper()
	writer, err := driver.Topic().StartWriter(topic,
		topicoptions.WithSyncWrite(true),
		topicoptions.WithWriterPartitionID(partitionID),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close(ctx)) }()

	topicMessages := make([]topicwriter.Message, 0, len(messages))
	for _, message := range messages {
		topicMessages = append(topicMessages, topicwriter.Message{Data: strings.NewReader(message)})
	}
	require.NoError(t, writer.Write(ctx, topicMessages...))
}

// CommittedOffset returns the consumer's committed offset for a partition.
func CommittedOffset(t testing.TB, ctx context.Context, driver *ydb.Driver, topic, consumer string, partitionID int64) int64 {
	t.Helper()
	description, err := driver.Topic().DescribeTopicConsumer(ctx, topic, consumer,
		topicoptions.IncludeConsumerStats())
	require.NoError(t, err)
	for _, partition := range description.Partitions {
		if partition.PartitionID == partitionID {
			return partition.PartitionConsumerStats.CommittedOffset
		}
	}
	t.Fatalf("partition %d not found", partitionID)
	return 0
}

// WaitCommittedOffset waits until a partition's committed offset reaches want or the timeout expires.
func WaitCommittedOffset(t testing.TB, ctx context.Context, driver *ydb.Driver, topic, consumer string, partitionID, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if CommittedOffset(t, ctx, driver, topic, consumer, partitionID) >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("partition %d committed offset did not reach %d; got %d", partitionID, want,
		CommittedOffset(t, ctx, driver, topic, consumer, partitionID))
}
