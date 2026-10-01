package processor_test

import (
	"aardappel/internal/dst_table"
	"aardappel/internal/hb_tracker"
	"aardappel/internal/processor"
	topic_reader "aardappel/internal/reader"
	"aardappel/internal/types"
	client "aardappel/internal/util/ydb"
	"aardappel/tests/common/local_ydb"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.uber.org/zap"
)

const integrationTimeout = 10 * time.Second

type replicationTestEnv struct {
	t          *testing.T
	ctx        context.Context
	client     *client.YdbClient
	streams    []replicationStream
	stateTable string
	instanceID string

	commitOffsetMode bool
	sessions         chan context.Context
	disconnect       chan context.CancelFunc
	readerErrors     chan error
}

type replicationStream struct {
	topic      string
	consumer   string
	dstTable   string
	partitions int
}

type processorRun struct {
	t         *testing.T
	ctx       context.Context
	cancel    context.CancelFunc
	readers   []*client.TopicReader
	done      []chan struct{}
	processor *processor.Processor
	dstTables []*dst_table.DstTable
	env       *replicationTestEnv
	enqueued  chan struct{}
}

type observedChannel struct {
	processor.Channel
	enqueued chan struct{}
}

type offsetCommitter interface {
	CommitOffset(context.Context, int64, int64) error
}

type offsetCommitterFunc func(context.Context, int64, int64) error

func (f offsetCommitterFunc) CommitOffset(ctx context.Context, partitionID int64, offset int64) error {
	return f(ctx, partitionID, offset)
}

func (c *observedChannel) EnqueueTx(ctx context.Context, data types.TxData) error {
	err := c.Channel.EnqueueTx(ctx, data)
	c.enqueued <- struct{}{}
	return err
}

func (c *observedChannel) EnqueueHb(ctx context.Context, data types.HbData) error {
	err := c.Channel.EnqueueHb(ctx, data)
	c.enqueued <- struct{}{}
	return err
}

func TestOffsetProgressIntegration(t *testing.T) {
	ydbSettings := local_ydb.NewYdbSettings()
	localYDB := local_ydb.StartupYdb(t, *ydbSettings)

	type testConfig struct {
		name             string
		commitOffsetMode bool
	}
	messageCommit := testConfig{name: "message_commit", commitOffsetMode: false}
	commitOffset := testConfig{name: "commit_offset", commitOffsetMode: true}

	t.Run("successful replication commits topic offset", func(t *testing.T) {
		for _, config := range []testConfig{messageCommit, commitOffset} {
			t.Run(config.name, func(t *testing.T) {
				env := newReplicationTestEnv(t, &localYDB, config.commitOffsetMode, processor.STAGE_RUN, 1)
				stream := env.streams[0]
				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0,
					`{"update":{"value":42},"key":[1],"ts":[1,1]}`,
					`{"resolved":[2,0]}`,
				)

				run := env.startProcessor()
				defer run.stop()
				_, err := run.processor.DoReplication(run.ctx, run.dstTables, run.executeTransaction)
				require.NoError(t, err)

				local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
					stream.topic, stream.consumer, 0, 2, integrationTimeout)
				require.Equal(t, uint64(42), env.readValue(0, 1))
				step, stage := env.readState()
				require.Equal(t, uint64(2), step)
				require.Equal(t, processor.STAGE_RUN, stage)
			})
		}
	})

	t.Run("session expires", func(t *testing.T) {
		for _, config := range []testConfig{messageCommit, commitOffset} {
			t.Run(config.name, func(t *testing.T) {
				env := newReplicationTestEnv(t, &localYDB, config.commitOffsetMode, processor.STAGE_RUN, 1)
				stream := env.streams[0]
				env.sessions = make(chan context.Context, 1)
				if !config.commitOffsetMode {
					env.readerErrors = make(chan error, 1)
				}
				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0,
					`{"update":{"value":42},"key":[1],"ts":[1,1]}`, `{"resolved":[2,0]}`)
				run := env.startProcessor()
				t.Cleanup(func() { run.stop() })
				run.waitEnqueued(2) // Keep the messages pending until their SDK session expires.
				env.expireSession()

				if !config.commitOffsetMode {
					require.Eventually(t, func() bool {
						return len(env.readerErrors) > 0 && run.ctx.Err() != nil
					}, integrationTimeout, 20*time.Millisecond, "expected shutdown after session expiry")
					var err *types.Error
					require.ErrorAs(t, <-env.readerErrors, &err)
					require.Equal(t, types.Graceful, err.Kind())
					run.stop()
					env.readerErrors = nil
					run = env.startProcessor() // Simulate the external supervisor restarting Aardappel.
				}

				_, err := run.processor.DoReplication(run.ctx, run.dstTables, run.executeTransaction)
				require.NoError(t, err)
				local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(), stream.topic, stream.consumer, 0, 2, integrationTimeout)
				require.Equal(t, uint64(42), env.readValue(0, 1))

				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0,
					`{"update":{"value":84},"key":[2],"ts":[3,1]}`, `{"resolved":[4,0]}`)
				_, err = run.processor.DoReplication(run.ctx, run.dstTables, run.executeTransaction)
				require.NoError(t, err)
				local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(), stream.topic, stream.consumer, 0, 4, integrationTimeout)
				require.Equal(t, uint64(84), env.readValue(0, 2))
				require.NoError(t, run.ctx.Err())
			})
		}
	})

	t.Run("replication resumes after transaction failure and restart", func(t *testing.T) {
		for _, config := range []testConfig{messageCommit, commitOffset} {
			t.Run(config.name, func(t *testing.T) {
				env := newReplicationTestEnv(t, &localYDB, config.commitOffsetMode, processor.STAGE_RUN, 1)
				stream := env.streams[0]
				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0,
					`{"update":{"value":42},"key":[1],"ts":[1,1]}`,
					`{"resolved":[2,0]}`,
				)

				failedRun := env.startProcessor()
				_, err := failedRun.processor.DoReplication(failedRun.ctx, failedRun.dstTables, failedRun.executeTransaction)
				require.NoError(t, err)
				local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
					stream.topic, stream.consumer, 0, 2, integrationTimeout)

				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0,
					`{"update":{"value":84},"key":[2],"ts":[3,1]}`,
					`{"resolved":[4,0]}`,
				)
				wantErr := errors.New("transaction failed")
				_, err = failedRun.processor.DoReplication(failedRun.ctx, failedRun.dstTables,
					func(func(context.Context, table.Session, table.Transaction) error) error { return wantErr })
				require.ErrorIs(t, err, wantErr)
				require.Equal(t, int64(2), local_ydb.CommittedOffset(t, env.ctx, env.client.GetDriver(),
					stream.topic, stream.consumer, 0))
				failedRun.stop()

				restartedRun := env.startProcessor()
				defer restartedRun.stop()
				_, err = restartedRun.processor.DoReplication(restartedRun.ctx, restartedRun.dstTables, restartedRun.executeTransaction)
				require.NoError(t, err)

				local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
					stream.topic, stream.consumer, 0, 4, integrationTimeout)
				require.Equal(t, uint64(42), env.readValue(0, 1))
				require.Equal(t, uint64(84), env.readValue(0, 2))
			})
		}
	})

	t.Run("topics and partitions commit offsets independently", func(t *testing.T) {
		env := newReplicationTestEnv(t, &localYDB, true, processor.STAGE_RUN, 1, 2)
		for streamID, stream := range env.streams {
			for partitionID := int64(0); partitionID < int64(stream.partitions); partitionID++ {
				id := uint64(streamID*2) + uint64(partitionID) + 1
				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, partitionID,
					fmt.Sprintf(`{"update":{"value":%d},"key":[%d],"ts":[%d,1]}`, id*11, id, id),
					`{"resolved":[10,0]}`,
				)
			}
		}

		blockedStarted := make(chan struct{})
		releaseBlocked := make(chan struct{})
		run := env.startProcessorWithCommits(10, func(committer offsetCommitter) offsetCommitter {
			return offsetCommitterFunc(func(ctx context.Context, partitionID int64, offset int64) error {
				if partitionID != 1 {
					return committer.CommitOffset(ctx, partitionID, offset)
				}
				close(blockedStarted)
				select {
				case <-releaseBlocked:
					return committer.CommitOffset(ctx, partitionID, offset)
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		})
		defer run.stop()
		_, err := run.processor.DoReplication(run.ctx, run.dstTables, run.executeTransaction)
		require.NoError(t, err)

		select {
		case <-blockedStarted:
		case <-time.After(integrationTimeout):
			t.Fatal("blocked partition commit did not start")
		}
		for _, stream := range env.streams {
			local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
				stream.topic, stream.consumer, 0, 1, integrationTimeout)
		}
		blockedStream := env.streams[1]
		require.Zero(t, local_ydb.CommittedOffset(t, env.ctx, env.client.GetDriver(),
			blockedStream.topic, blockedStream.consumer, 1))
		for streamID, stream := range env.streams {
			for partitionID := int64(0); partitionID < int64(stream.partitions); partitionID++ {
				id := uint64(streamID*2) + uint64(partitionID) + 1
				require.Equal(t, id*11, env.readValue(streamID, id))
			}
		}

		close(releaseBlocked)
		local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
			blockedStream.topic, blockedStream.consumer, 1, 1, integrationTimeout)
	})

	t.Run("slow offset commits do not block destination writes", func(t *testing.T) {
		env := newReplicationTestEnv(t, &localYDB, true, processor.STAGE_RUN, 2)
		stream := env.streams[0]
		started := make(chan types.TopicOffset, 10)
		release := make(chan struct{})
		var active atomic.Int32
		var exceededLimit atomic.Bool
		run := env.startProcessorWithCommits(1, func(committer offsetCommitter) offsetCommitter {
			return offsetCommitterFunc(func(ctx context.Context, partitionID int64, offset int64) error {
				if active.Add(1) > 1 {
					exceededLimit.Store(true)
				}
				defer active.Add(-1)
				select {
				case started <- types.TopicOffset{PartitionId: partitionID, Offset: offset - 1}:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case <-release:
					return committer.CommitOffset(ctx, partitionID, offset)
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		})
		defer run.stop()
		waitStarted := func() types.TopicOffset {
			t.Helper()
			select {
			case offset := <-started:
				return offset
			case <-time.After(integrationTimeout):
				t.Fatal("offset commit did not start")
				return types.TopicOffset{}
			}
		}
		writeAndReplicate := func(round int) {
			t.Helper()
			for partition := 0; partition < 2; partition++ {
				id := round*2 + partition + 1
				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, int64(partition),
					fmt.Sprintf(`{"update":{"value":%d},"key":[%d],"ts":[%d,1]}`, id, id, round*2+1),
					fmt.Sprintf(`{"resolved":[%d,0]}`, round*2+2))
			}
			run.waitEnqueued(4)
			_, err := run.processor.DoReplication(run.ctx, run.dstTables, run.executeTransaction)
			require.NoError(t, err)
		}

		writeAndReplicate(0)
		first := waitStarted()
		// The only commit slot is occupied while the next batch reaches destination.
		writeAndReplicate(1)
		for id := uint64(1); id <= 4; id++ {
			require.Equal(t, id, env.readValue(0, id))
		}
		for partition := int64(0); partition < 2; partition++ {
			require.Zero(t, local_ydb.CommittedOffset(t, env.ctx, env.client.GetDriver(),
				stream.topic, stream.consumer, partition))
		}
		require.Equal(t, int32(1), active.Load())
		release <- struct{}{}
		waitStarted()
		local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
			stream.topic, stream.consumer, first.PartitionId, first.Offset+1, integrationTimeout)
		require.Zero(t, local_ydb.CommittedOffset(t, env.ctx, env.client.GetDriver(),
			stream.topic, stream.consumer, 1-first.PartitionId))

		close(release)
		for partition := int64(0); partition < 2; partition++ {
			// Offset 3 acknowledges both data messages; the last heartbeat may remain pending.
			local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
				stream.topic, stream.consumer, partition, 3, integrationTimeout)
		}
		require.False(t, exceededLimit.Load(), "more than one offset commit ran concurrently")
	})

	t.Run("initial scan completes with expected data, state and offsets", func(t *testing.T) {
		for _, config := range []testConfig{messageCommit, commitOffset} {
			t.Run(config.name, func(t *testing.T) {
				env := newReplicationTestEnv(t, &localYDB, config.commitOffsetMode, processor.STAGE_INITIAL_SCAN, 1)
				stream := env.streams[0]
				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0,
					`{"update":{"value":42},"key":[1],"ts":[1,1]}`,
					`{"resolved":[2,0]}`,
				)

				run := env.startProcessor()
				defer run.stop()
				run.waitEnqueued(2)
				initialMessagesProcessed := make(chan struct{})
				run.processor.Enqueue(run.ctx, func() error {
					close(initialMessagesProcessed)
					return nil
				})

				result := make(chan error, 1)
				go func() {
					_, err := run.processor.DoReplication(run.ctx, run.dstTables, run.executeTransaction)
					result <- err
				}()
				select {
				case <-initialMessagesProcessed:
				case <-time.After(integrationTimeout):
					t.Fatal("initial messages were not processed")
				}
				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0,
					`{"resolved":[3,0]}`)

				select {
				case err := <-result:
					require.NoError(t, err)
				case <-time.After(integrationTimeout):
					t.Fatal("initial scan did not finish")
				}

				local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
					stream.topic, stream.consumer, 0, 3, integrationTimeout)
				require.Equal(t, uint64(42), env.readValue(0, 1))
				step, stage := env.readState()
				require.Equal(t, uint64(3), step)
				require.Equal(t, processor.STAGE_RUN, stage)
			})
		}
	})

	t.Run("initial scan continues after the thousand transaction batch limit", func(t *testing.T) {
		for _, config := range []testConfig{messageCommit, commitOffset} {
			t.Run(config.name, func(t *testing.T) {
				env := newReplicationTestEnv(t, &localYDB, config.commitOffsetMode, processor.STAGE_INITIAL_SCAN, 1)
				stream := env.streams[0]
				messages := make([]string, 0, 1002)
				for id := 1; id <= 1001; id++ {
					messages = append(messages, fmt.Sprintf(`{"update":{"value":%d},"key":[%d],"ts":[1,%d]}`, id, id, id))
				}
				messages = append(messages, `{"resolved":[2,0]}`)
				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0, messages...)

				run := env.startProcessor()
				defer run.stop()
				run.waitEnqueued(1000)
				_, err := run.processor.DoReplication(run.ctx, run.dstTables, run.executeTransaction)
				require.NoError(t, err)
				local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
					stream.topic, stream.consumer, 0, 1000, integrationTimeout)
				_, stage := env.readState()
				require.Equal(t, processor.STAGE_INITIAL_SCAN, stage)

				run.waitEnqueued(2)
				remainingMessagesProcessed := make(chan struct{})
				run.processor.Enqueue(run.ctx, func() error {
					close(remainingMessagesProcessed)
					return nil
				})
				result := make(chan error, 1)
				go func() {
					_, err := run.processor.DoReplication(run.ctx, run.dstTables, run.executeTransaction)
					result <- err
				}()
				select {
				case <-remainingMessagesProcessed:
				case <-time.After(integrationTimeout):
					t.Fatal("remaining initial-scan messages were not processed")
				}
				local_ydb.WriteTopicMessages(t, env.ctx, env.client.GetDriver(), stream.topic, 0,
					`{"resolved":[3,0]}`)
				select {
				case err := <-result:
					require.NoError(t, err)
				case <-time.After(integrationTimeout):
					t.Fatal("initial scan did not finish")
				}

				local_ydb.WaitCommittedOffset(t, env.ctx, env.client.GetDriver(),
					stream.topic, stream.consumer, 0, 1003, integrationTimeout)
				require.Equal(t, uint64(1001), env.readValue(0, 1001))
				step, stage := env.readState()
				require.Equal(t, uint64(3), step)
				require.Equal(t, processor.STAGE_RUN, stage)
			})
		}
	})
}

func newReplicationTestEnv(t *testing.T, localYDB *local_ydb.Ydb, commitOffsetMode bool, stage string, partitionCounts ...int) *replicationTestEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	disconnect := make(chan context.CancelFunc, 1)
	ydbClient, err := client.NewYdbClient(ctx, zap.NewNop(), localYDB.ConnectionString(),
		ydb.WithBalancer(balancers.SingleConn()), ydb.WithTraceDriver(trace.Driver{
			OnConnNewStream: func(info trace.DriverConnNewStreamStartInfo) func(trace.DriverConnNewStreamDoneInfo) {
				if info.Method.Name() == "StreamRead" {
					var cancel context.CancelFunc
					*info.Context, cancel = context.WithCancel(*info.Context)
					select {
					case disconnect <- cancel:
					default:
					}
				}
				return nil
			},
		}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ydbClient.GetDriver().Close(context.Background())) })

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	env := &replicationTestEnv{
		t:                t,
		ctx:              ctx,
		client:           ydbClient,
		stateTable:       "offset_state_" + suffix,
		instanceID:       "offset_instance",
		commitOffsetMode: commitOffsetMode,
		disconnect:       disconnect,
	}
	for streamID, partitions := range partitionCounts {
		stream := replicationStream{
			topic:      fmt.Sprintf("offset_topic_%s_%d", suffix, streamID),
			consumer:   "offset_consumer",
			dstTable:   fmt.Sprintf("/local/offset_dst_%s_%d", suffix, streamID),
			partitions: partitions,
		}
		env.streams = append(env.streams, stream)
		local_ydb.CreateTopic(t, ctx, ydbClient.GetDriver(), stream.topic, stream.consumer, int64(partitions))
		env.createDestinationTable(streamID)
	}
	env.createStateTable(stage)
	return env
}

func (f *replicationTestEnv) createStateTable(stage string) {
	f.t.Helper()
	f.executeScheme(fmt.Sprintf("CREATE TABLE `%s` (id Utf8, step_id Uint64, tx_id Uint64, state Utf8, stage Utf8, last_msg Utf8, PRIMARY KEY(id))", f.stateTable))
	params := table.NewQueryParameters(
		table.ValueParam("$id", ydb_types.UTF8Value(f.instanceID)),
		table.ValueParam("$state", ydb_types.UTF8Value(processor.REPLICATION_OK)),
		table.ValueParam("$stage", ydb_types.UTF8Value(stage)),
	)
	err := f.client.TableClient.DoTx(f.ctx, func(ctx context.Context, tx table.TransactionActor) error {
		_, err := tx.Execute(ctx,
			fmt.Sprintf("UPSERT INTO `%s` (id, step_id, tx_id, state, stage) VALUES ($id, 0, 0, $state, $stage)", f.stateTable),
			params,
		)
		return err
	})
	require.NoError(f.t, err)
}

func (f *replicationTestEnv) createDestinationTable(streamID int) {
	f.t.Helper()
	f.executeScheme(fmt.Sprintf("CREATE TABLE `%s` (id Uint64, value Uint64, PRIMARY KEY(id))",
		f.streams[streamID].dstTable))
}

func (f *replicationTestEnv) executeScheme(query string) {
	f.t.Helper()
	err := f.client.TableClient.Do(f.ctx, func(ctx context.Context, session table.Session) error {
		return session.ExecuteSchemeQuery(ctx, query, nil)
	})
	require.NoError(f.t, err)
}

func (f *replicationTestEnv) startProcessor() *processorRun {
	return f.startProcessorWithCommits(10, nil)
}

func (f *replicationTestEnv) expireSession() {
	f.t.Helper()
	require.Eventually(f.t, func() bool { return len(f.sessions) > 0 }, integrationTimeout, 20*time.Millisecond, "session did not start")
	session := <-f.sessions
	require.NoError(f.t, session.Err())
	stream := f.streams[0]
	// Break the SDK's read stream; it must expire the session and reconnect itself.
	require.Eventually(f.t, func() bool { return len(f.disconnect) > 0 }, integrationTimeout, 20*time.Millisecond, "read stream did not start")
	(<-f.disconnect)()
	require.Eventually(f.t, func() bool { return session.Err() != nil }, integrationTimeout, 20*time.Millisecond, "session did not expire")
	require.Zero(f.t, local_ydb.CommittedOffset(f.t, f.ctx, f.client.GetDriver(), stream.topic, stream.consumer, 0))
}

func (f *replicationTestEnv) startProcessorWithCommits(maxConcurrentOffsetCommits int, wrapReader func(offsetCommitter) offsetCommitter) *processorRun {
	f.t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	streamLayout := hb_tracker.TopicPartsCount{
		TopicPartsCountMap: make(map[int]hb_tracker.StreamCfg, len(f.streams)),
	}
	dstTables := make([]*dst_table.DstTable, 0, len(f.streams))
	for streamID, stream := range f.streams {
		streamLayout.TopicPartsCountMap[streamID] = hb_tracker.StreamCfg{PartitionsCount: stream.partitions}
		streamLayout.TotalPartsCount += stream.partitions
		dstTable := dst_table.NewDstTable(f.client.TableClient, stream.dstTable, "test")
		require.NoError(f.t, dstTable.Init(ctx))
		dstTables = append(dstTables, dstTable)
	}
	prc, err := processor.NewProcessor(ctx, streamLayout,
		f.stateTable, f.client.TableClient, f.instanceID, nil, f.commitOffsetMode, maxConcurrentOffsetCommits)
	require.NoError(f.t, err)

	enqueued := make(chan struct{}, 10)
	channel := &observedChannel{Channel: prc, enqueued: enqueued}
	readers := make([]*client.TopicReader, 0, len(f.streams))
	done := make([]chan struct{}, 0, len(f.streams))
	errChannel := make(chan error)
	errorHandlerDone := make(chan struct{})
	done = append(done, errorHandlerDone)
	go func() {
		defer close(errorHandlerDone)
		for {
			select {
			case err := <-errChannel:
				if f.readerErrors != nil {
					f.readerErrors <- err
				} else if ctx.Err() == nil {
					f.t.Errorf("topic reader error: %v", err)
				}
				cancel()
			case <-ctx.Done():
				return
			}
		}
	}()
	for streamID, stream := range f.streams {
		var readerOptions []topicoptions.ReaderOption
		if f.sessions != nil {
			readerOptions = append(readerOptions, topicoptions.WithReaderTrace(trace.Topic{
				OnReaderPartitionReadStartResponse: func(info trace.TopicReaderPartitionReadStartResponseStartInfo) func(trace.TopicReaderPartitionReadStartResponseDoneInfo) {
					select {
					case f.sessions <- *info.PartitionContext:
					default:
					}
					return nil
				},
			}))
		}
		var updateCb topic_reader.UpdateOffsetFunc
		if !f.commitOffsetMode {
			var startCb topicoptions.GetPartitionStartOffsetFunc
			startCb, updateCb = topic_reader.MakeTopicReaderGuard(errChannel)
			readerOptions = append(readerOptions, topicoptions.WithReaderGetPartitionStartOffset(startCb))
		}
		reader, err := f.client.TopicClient.StartReader(stream.consumer, stream.topic, readerOptions...)
		require.NoError(f.t, err)
		if f.commitOffsetMode {
			var committer offsetCommitter = reader
			if wrapReader != nil {
				committer = wrapReader(committer)
			}
			prc.RegisterTopicReader(uint32(streamID), committer)
		}
		readerDone := make(chan struct{})
		readers = append(readers, reader)
		done = append(done, readerDone)
		go func(streamID uint32, stream replicationStream, reader *client.TopicReader, readerDone chan struct{}) {
			defer close(readerDone)
			topic_reader.ReadTopic(ctx, topic_reader.StreamInfo{
				Id:              streamID,
				TopicPath:       stream.topic,
				PartCount:       stream.partitions,
				ProblemStrategy: types.ProblemStrategyStop,
			}, reader, channel, nil, updateCb, nil, errChannel, f.commitOffsetMode)
		}(uint32(streamID), stream, reader, readerDone)
	}
	return &processorRun{f.t, ctx, cancel, readers, done, prc, dstTables, f, enqueued}
}

func (r *processorRun) executeTransaction(fn func(context.Context, table.Session, table.Transaction) error) error {
	return r.env.client.TableClient.Do(r.ctx, func(ctx context.Context, session table.Session) error {
		tx, err := session.BeginTransaction(ctx, table.TxSettings(table.WithSerializableReadWrite()))
		if err != nil {
			return err
		}
		return fn(ctx, session, tx)
	})
}

func (r *processorRun) stop() {
	r.cancel()
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, reader := range r.readers {
		_ = reader.Close(closeCtx)
	}
	for _, done := range r.done {
		select {
		case <-done:
		case <-closeCtx.Done():
			r.t.Fatal("topic reader did not stop")
		}
	}
}

func (r *processorRun) waitEnqueued(count int) {
	r.t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-r.enqueued:
		case <-time.After(integrationTimeout):
			r.t.Fatalf("only %d of %d topic messages were enqueued", i, count)
		}
	}
}

func (f *replicationTestEnv) readValue(streamID int, id uint64) uint64 {
	f.t.Helper()
	var value *uint64
	err := f.client.TableClient.DoTx(f.ctx, func(ctx context.Context, tx table.TransactionActor) error {
		result, err := tx.Execute(ctx, fmt.Sprintf("SELECT value FROM `%s` WHERE id = %d",
			f.streams[streamID].dstTable, id), nil)
		if err != nil {
			return err
		}
		if !result.NextResultSet(ctx) || !result.NextRow() {
			return fmt.Errorf("row %d not found", id)
		}
		return result.ScanNamed(named.Optional("value", &value))
	})
	require.NoError(f.t, err)
	require.NotNil(f.t, value)
	return *value
}

func (f *replicationTestEnv) readState() (uint64, string) {
	f.t.Helper()
	var step *uint64
	var stage *string
	err := f.client.TableClient.DoTx(f.ctx, func(ctx context.Context, tx table.TransactionActor) error {
		result, err := tx.Execute(ctx,
			fmt.Sprintf("SELECT step_id, stage FROM `%s` WHERE id = \"%s\"", f.stateTable, f.instanceID), nil)
		if err != nil {
			return err
		}
		if !result.NextResultSet(ctx) || !result.NextRow() {
			return fmt.Errorf("replication state not found")
		}
		return result.ScanNamed(named.Optional("step_id", &step), named.Optional("stage", &stage))
	})
	require.NoError(f.t, err)
	require.NotNil(f.t, step)
	require.NotNil(f.t, stage)
	return *step, *stage
}
