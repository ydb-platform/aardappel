package processor

import (
	"aardappel/internal/dst_table"
	"aardappel/internal/hb_tracker"
	"aardappel/internal/tx_queue"
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"go.uber.org/zap"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const REPLICATION_OK = "OK"
const REPLICATION_FATAL_ERROR = "FATAL_ERROR"

const STAGE_UNDEFINED = "UNDEFINED"
const STAGE_INITIAL_SCAN = "INITIAL_SCAN"
const STAGE_RUN = "RUN"

type Processor struct {
	txChannel       chan func() error
	hbTracker       *hb_tracker.HeartBeatTracker
	txQueue         *tx_queue.TxQueue
	dstServerClient table.Client
	lastStep        atomic.Uint64
	stateStoreQuery string
	stateTablePath  string
	instanceId      string
	stage           string
	initialScanPos  *types.HbData
}

type RequestStats struct {
	ModificationsCount int
	RequestSize        int
}

type ReplicationStats struct {
	ModificationsCount      int
	LastHeartBeat           uint64
	CommitDurationMs        int64
	RequestSize             int
	QuorumWaitingDurationMs int64
}

type Channel interface {
	EnqueueTx(ctx context.Context, data types.TxData)
	EnqueueHb(ctx context.Context, heartbeat types.HbData)
	SaveReplicationState(ctx context.Context, state string, lastError string) error
}

type ConflictHandler interface {
	// returns
	// -1 - not found
	//  0 - skip
	//  1 - appply
	Handle(ctx context.Context, topicPath string, serializeKey string, step uint64, txId uint64) int
}

type Cmd struct {
	InstanceId string            `json:"aardapel_instance_id"`
	Key        []json.RawMessage `json:"key"`
	TS         []uint64          `json:"ts"`
	Action     string            `json:"action"`
	Path       string            `json:"path"`
}

type CmdQueueConflictHandler struct {
	InstanceId string
	Path       string
	Consumer   string
	Topic      topic.Client
	Lock       sync.Mutex
}

func NewCmdQueueConflictHandler(ctx context.Context, instanceId string, path string, consumer string, topic topic.Client) *CmdQueueConflictHandler {
	var handler CmdQueueConflictHandler
	handler.InstanceId = instanceId
	handler.Path = path
	handler.Consumer = consumer
	handler.Topic = topic
	return &handler
}

func readWithTimeout(ctx context.Context, reader *topicreader.Reader) (*topicreader.Message, error, bool) {
	timingCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	msg, err := reader.ReadMessage(timingCtx)
	if timingCtx.Err() != nil {
		return nil, nil, true
	}
	return msg, err, false
}

func (this *CmdQueueConflictHandler) Handle(ctx context.Context, streamTopicPath string, key string, step uint64, txId uint64) int {
	this.Lock.Lock()
	defer this.Lock.Unlock()

	reader, err := this.Topic.StartReader(this.Consumer, topicoptions.ReadTopic(this.Path))
	if err != nil {
		xlog.Fatal(ctx, "Unable to read from cpecifyed command topic",
			zap.String("consumer", this.Consumer),
			zap.String("src_topic", this.Path),
			zap.Error(err))
		return -1
	}

	defer func() {
		err := reader.Close(ctx)
		xlog.Error(ctx, "stop reader call returns", zap.Error(err))
	}()

	//TODO: Move to common place
	serializeKey := func(key []json.RawMessage) (string, error) {
		data, err := json.Marshal(key)
		if err != nil {
			return "underfined", err
		}
		return string(data), nil
	}

	var lastCmd *Cmd
	for ctx.Err() == nil {
		msg, err, timeout := readWithTimeout(ctx, reader)
		if timeout == true {
			break
		}

		if err != nil {
			xlog.Error(ctx, "Unable to read message", zap.Error(err))
			return -1
		}
		jsonData, err := io.ReadAll(msg)
		if err != nil {
			xlog.Error(ctx, "Unable to read all", zap.Error(err))
			return -1
		}

		var cmd Cmd
		err = json.Unmarshal(jsonData, &cmd)
		if err != nil || len(cmd.TS) != 2 {
			xlog.Error(ctx, "Unable to parse command", zap.ByteString("json", jsonData), zap.Error(err))
			continue
		}

		cmdKey, err := serializeKey(cmd.Key)
		if err != nil {
			xlog.Error(ctx, "Unable to serialize key from command, skip the command", zap.Error(err))
			continue
		}

		if cmd.InstanceId == this.InstanceId && cmd.Path == streamTopicPath && cmdKey == key && step == cmd.TS[0] && txId == cmd.TS[1] {
			if cmd.Action != "skip" && cmd.Action != "apply" {
				xlog.Debug(ctx, "invalid command", zap.String("action", cmd.Action))
			} else {
				xlog.Debug(ctx, "External instruction found",
					zap.String("topic", cmd.Path),
					zap.String("key", cmdKey),
					zap.Uint64("step", cmd.TS[0]),
					zap.Uint64("txId", cmd.TS[1]),
					zap.String("action", cmd.Action))

				lastCmd = &cmd
			}
		}
	}
	if lastCmd != nil {
		if lastCmd.Action == "skip" {
			return 0
		}
		if lastCmd.Action == "apply" {
			return 1
		}
	}
	return -1
}

type TxBatch struct {
	TxData []types.TxData
	Hb     types.HbData
}

func createStateStoreQuery(stateTable string) string {
	return fmt.Sprintf(`
UPSERT INTO
    %v
    (id, step_id, tx_id, stage)
VALUES
    ($instanceId, $stateStepId, $stateTxId, $stage);
`, stateTable)
}

type NoInstance struct {
	instanceId string
}

func (e *NoInstance) Error() string {
	return "No instance id found" + e.instanceId
}

type ReplicationState struct {
	stage string
	step  uint64
}

func selectReplicationState(ctx context.Context, client table.Client, stateTablePath string, instanceId string) (ReplicationState, error) {
	param := table.NewQueryParameters(
		table.ValueParam("$instanceId", ydbTypes.UTF8Value(instanceId)),
	)
	stateQuery := fmt.Sprintf("SELECT step_id, tx_id, state, stage, last_msg FROM %v WHERE id = $instanceId;", stateTablePath)
	var step *uint64
	var tx_id *uint64
	var state *string
	var lastMsg *string
	var stage *string

	err := client.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, stateQuery, param)
			if err != nil {
				return err
			}
			res.NextResultSet(ctx)
			if res.NextRow() == false {
				return &NoInstance{instanceId}
			}
			return res.ScanNamed(
				named.Optional("step_id", &step),
				named.Optional("tx_id", &tx_id),
				named.Optional("state", &state),
				named.Optional("stage", &stage),
				named.Optional("last_msg", &lastMsg),
			)
		})

	if err != nil {
		return ReplicationState{step: 0, stage: STAGE_UNDEFINED}, fmt.Errorf("unable to get state table: %v %w", stateTablePath, err)
	}

	if state == nil {
		return ReplicationState{step: 0, stage: STAGE_UNDEFINED}, fmt.Errorf("State is not set in the state table")
	}

	if step == nil || tx_id == nil {
		return ReplicationState{step: 0, stage: STAGE_UNDEFINED}, fmt.Errorf("virtual timestamp is not set in the state table")
	}

	if *state != REPLICATION_OK {
		return ReplicationState{step: 0, stage: STAGE_UNDEFINED}, fmt.Errorf("Stored replication status is not ok. last_msg: %s, state: %s", *lastMsg, *state)
	}

	return ReplicationState{step: *step, stage: *stage}, err
}

func NewProcessor(ctx context.Context, total int, stateTablePath string, client table.Client, instanceId string) (*Processor, error) {
	var p Processor
	p.hbTracker = hb_tracker.NewHeartBeatTracker(total)
	p.txChannel = make(chan func() error, 1000)
	p.txQueue = tx_queue.NewTxQueue()
	p.dstServerClient = client
	p.stateStoreQuery = createStateStoreQuery(stateTablePath)
	p.stateTablePath = stateTablePath
	p.instanceId = instanceId

	if len(instanceId) == 0 {
		return nil, errors.New("instance_id must be set")
	}

	state, err := selectReplicationState(ctx, p.dstServerClient, stateTablePath, p.instanceId)

	if err != nil {
		return nil, err
	}
	p.lastStep.Store(state.step)
	p.stage = state.stage
	xlog.Debug(ctx, "processor created", zap.Uint64("last step", state.step))
	return &p, err
}

func (processor *Processor) StartHbGuard(ctx context.Context, timeout uint32, streamDbgInfos []string) {
	processor.hbTracker.StartHbGuard(ctx, timeout, streamDbgInfos)
}

func (processor *Processor) Enqueue(ctx context.Context, fn func() error) {
	// To be able to handle restart we must no block after ctx cancellation
	select {
	case processor.txChannel <- fn:
		return
	case <-ctx.Done():
		return
	}
}

func (processor *Processor) EnqueueHb(ctx context.Context, hb types.HbData) {
	// Skip all before we already processed
	lastStep := processor.lastStep.Load()
	xlog.Debug(ctx, "got hb", zap.Uint64("step", hb.Step),
		zap.Uint32("reader_id", hb.StreamId.ReaderId),
		zap.Int64("partitionId:", hb.StreamId.PartitionId),
		zap.Bool("willSkip", hb.Step < lastStep))
	if hb.Step < lastStep {
		err := hb.CommitTopic()
		xlog.Debug(ctx, "skip old hb",
			zap.Uint64("step", hb.Step),
			zap.NamedError("topic commit error", err))
		return
	}
	fn := func() error {
		step := processor.lastStep.Load()
		if hb.Step < step {
			xlog.Warn(ctx, "suspicious behaviour, hb with step less then our last committed step has been "+
				"enqueued just during our commit",
				zap.Uint64("step", hb.Step),
				zap.Uint64("ourStep", step))
			return nil
		}
		return processor.hbTracker.AddHb(hb)
	}

	processor.Enqueue(ctx, fn)
}

func (processor *Processor) SaveReplicationState(ctx context.Context, status string, lastError string) error {
	param := table.NewQueryParameters(
		table.ValueParam("$instanceId", ydbTypes.UTF8Value(processor.instanceId)),
		table.ValueParam("$state", ydbTypes.UTF8Value(status)),
		table.ValueParam("$lastError", ydbTypes.UTF8Value(lastError)),
	)

	stopQuery := fmt.Sprintf("UPSERT INTO %v (id, state, last_msg) VALUES ($instanceId,$state,$lastError)",
		processor.stateTablePath)

	return processor.dstServerClient.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			_, err := tx.Execute(ctx, stopQuery, param)
			return err
		})
}

func (processor *Processor) EnqueueTx(ctx context.Context, tx types.TxData) {
	// Skip all before we already processed
	lastStep := processor.lastStep.Load()
	xlog.Debug(ctx, "got tx", zap.Uint64("step", tx.Step),
		zap.Uint64("txId", tx.TxId),
		zap.Uint32("reader_id", tx.TableId),
		zap.Bool("willSkip", tx.Step < lastStep))
	if tx.Step < lastStep {
		err := tx.CommitTopic()
		xlog.Debug(ctx, "skip old tx",
			zap.Uint64("step", tx.Step),
			zap.NamedError("topic commit error", err))
		return
	}
	fn := func() error {
		step := processor.lastStep.Load()
		if tx.Step < step {
			xlog.Warn(ctx, "suspicious behaviour, tx with step less then our last committed step has been"+
				"enqueued just during our commit",
				zap.Uint64("step", tx.Step),
				zap.Uint64("ourStep", step))
			return nil
		}
		processor.txQueue.PushTx(tx)
		return nil
	}

	processor.Enqueue(ctx, fn)
}

func (processor *Processor) getQueryRequestSize(query dst_table.PushQuery) int {
	var size int
	size += len(query.Query)
	size += len(query.Parameters.String())
	return size
}

func (processor *Processor) assignTxsToDstTables(ctx context.Context, txs []types.TxData, dstTables []*dst_table.DstTable) (dst_table.PushQuery, RequestStats, error) {
	txDataPerTable := make([][]types.TxData, len(dstTables))
	for i := 0; i < len(txs); i++ {
		txDataPerTable[txs[i].TableId] = append(txDataPerTable[txs[i].TableId], txs[i])
	}
	if len(txDataPerTable) != len(dstTables) {
		return dst_table.PushQuery{}, RequestStats{}, fmt.Errorf("Count of tables in dst database and count of tables in the txs mismatched, txDataPertabe: %d, dstTable: %d",
			len(txDataPerTable), len(dstTables))
	}
	var query dst_table.PushQuery
	var modifications int
	for i := 0; i < len(txDataPerTable); i++ {
		q, err := dstTables[i].GenQuery(ctx, txDataPerTable[i], i)
		if err != nil {
			return dst_table.PushQuery{}, RequestStats{}, fmt.Errorf("%w; Unable to generate query", err)
		}
		query.Query += q.Query
		modifications += q.ModificationsCount
		query.Parameters = append(query.Parameters, q.Parameters...)

	}
	size := processor.getQueryRequestSize(query)
	xlog.Debug(ctx, "Query to perform", zap.String("query", query.Query))

	return query, RequestStats{modifications, size}, nil
}

func (processor *Processor) doEvent(ctx context.Context) error {
	var maxEventPerIteration int = 1000
	for maxEventPerIteration > 0 {
		maxEventPerIteration--
		select {
		case fn := <-processor.txChannel:
			err := fn()
			if err != nil {
				xlog.Debug(ctx, "Unable to push event", zap.Error(err))
				return err
			}
		default:
			maxEventPerIteration = 0
		}
	}
	return nil
}

func (processor *Processor) getHbQuorum(ctx context.Context) (*types.HbData, error) {
	// Try to get any quorum.
	err := processor.doEvent(ctx)
	if err != nil {
		return nil, err
	}
	hb, ok := processor.hbTracker.GetQuorum()
	if ok {
		xlog.Debug(ctx, "Got hb quorum", zap.Any("step", hb.Step))
		return &hb, nil
	}
	return nil, nil
}

func (processor *Processor) getHbQuorumAfter(ctx context.Context, hb types.HbData) (*types.HbData, error) {
	// Try to get quorum that will be large then hb.Step.
	err := processor.doEvent(ctx)
	if err != nil {
		return nil, err
	}
	resHb, ok := processor.hbTracker.GetQuorumAfter(hb)
	if ok {
		xlog.Debug(ctx, "Got hb quorum after", zap.Any("res_step", resHb.Step), zap.Any("after_step", hb.Step))
		return &resHb, nil
	}
	return nil, nil
}

func (processor *Processor) waitHbQuorum(ctx context.Context) (types.HbData, error) {
	// Wait any quorum.
	for ctx.Err() == nil {
		hb, err := processor.getHbQuorum(ctx)
		if err != nil {
			return types.HbData{}, err
		}
		if hb != nil {
			return *hb, nil
		}
		//TODO: Wait any hb instead of sleep here
		time.Sleep(1 * time.Millisecond)
	}
	return types.HbData{}, ctx.Err()
}

func (processor *Processor) waitSyncHbQuorum(ctx context.Context, hb types.HbData) (types.HbData, error) {
	// Wait quorum that will be large then hb.Step.
	for ctx.Err() == nil {
		resHb, err := processor.getHbQuorumAfter(ctx, hb)
		if err != nil {
			return types.HbData{}, err
		}
		if resHb != nil {
			return *resHb, nil
		}
		//TODO: Wait any hb instead of sleep here
		time.Sleep(1 * time.Millisecond)
	}
	return types.HbData{}, ctx.Err()
}

func (processor *Processor) getSyncHbQuorum(ctx context.Context) (*types.HbData, error) {
	// Get quorum after that initial scan will be finished and database will be consistent.
	// This quorum is the first larger quorum than the max hb step in the first quorum obtained in initial scan stage.
	err := processor.doEvent(ctx)
	if err != nil {
		return nil, err
	}
	quorumExist := processor.hbTracker.GetReady()
	if !quorumExist {
		return nil, nil
	}
	maxHb := processor.hbTracker.GetMaxHb()
	hb, err := processor.waitSyncHbQuorum(ctx, maxHb)
	if err != nil {
		return nil, err
	}
	return &hb, nil
}

func (processor *Processor) DoInitialScan(ctx context.Context, dstTables []*dst_table.DstTable,
	lockExecutor func(fn func(context.Context, table.Session, table.Transaction) error) error) (*ReplicationStats, error) {

	quorumWaitingDuration := time.Now().UnixMilli()
	if processor.initialScanPos == nil {
		// Trying to get a quorum, up to which there will be an initial scan state.
		// This quorum should be greater than the max hb step in the first quorum obtained during the initial scan
		hb, err := processor.getSyncHbQuorum(ctx)
		if err != nil {
			return nil, err
		}
		if hb != nil {
			xlog.Debug(ctx, "Got sync hb", zap.Any("step", hb.Step))
		}
		// Save this quorum if it is existed
		processor.initialScanPos = hb
	}
	quorumWaitingDuration = time.Now().UnixMilli() - quorumWaitingDuration

	maxCount := 1000
	var txs []types.TxData
	if processor.initialScanPos != nil {
		// We should push transactions smaller than initialScanPos.Step and no more than maxCount transactions
		xlog.Debug(ctx, "Trying to pop tx until", zap.Any("step", processor.initialScanPos.Step), zap.Any("max_count", maxCount))
		txs = processor.txQueue.PopTxsByCountAndStep(processor.initialScanPos.Step, maxCount)
	} else {
		// We can push no more than maxCount transactions
		xlog.Debug(ctx, "Trying to pop tx until", zap.Any("max_count", maxCount))
		txs = processor.txQueue.PopTxsByCount(maxCount)
	}

	// If it is the last iteration we need to save quorum and switch to RUN stage mode.
	// Otherwise, it is only necessary to push the transactions and commit them to the topic.
	lastInitialScanIt := processor.initialScanPos != nil && len(txs) < maxCount

	if lastInitialScanIt {
		processor.hbTracker.Commit(*processor.initialScanPos)
	}
	for _, data := range txs {
		xlog.Debug(ctx, "Parsed tx data",
			zap.Any("column_values", data.ColumnValues),
			zap.String("operation_type", data.OperationType.String()),
			zap.Any("key", data.KeyValues),
			zap.Uint64("step", data.Step),
			zap.Uint64("tx_id", data.TxId),
			zap.Uint32("tableId:", data.TableId))
	}

	query, requestStats, err := processor.assignTxsToDstTables(ctx, txs, dstTables)
	if err != nil {
		return nil, err
	}
	commitDuration := time.Now().UnixMilli()

	if lastInitialScanIt {
		err := lockExecutor(func(ctx context.Context, ts table.Session, txr table.Transaction) error {
			return processor.PushAsSingleTx(ctx, query, txr, types.Position{Step: processor.initialScanPos.Step, TxId: 0}, STAGE_RUN)
		})
		commitDuration = time.Now().UnixMilli() - commitDuration
		if err != nil {
			return nil, fmt.Errorf("%w; Unable to push tx with state", err)
		}

		processor.lastStep.Store(processor.initialScanPos.Step)
		processor.stage = STAGE_RUN
	} else if len(txs) != 0 {
		err := lockExecutor(func(ctx context.Context, ts table.Session, txr table.Transaction) error {
			return processor.PushTxs(ctx, query, txr)
		})
		commitDuration = time.Now().UnixMilli() - commitDuration
		if err != nil {
			return nil, fmt.Errorf("%w; Unable to push tx", err)
		}
	}

	for i := 0; i < len(txs); i++ {
		err := txs[i].CommitTopic()
		if err != nil {
			return nil, fmt.Errorf("%w; Unable to commit topic fot dataTx", err)
		}
	}

	if lastInitialScanIt {
		xlog.Debug(ctx, "commit hb in topic", zap.Uint64("step", processor.initialScanPos.Step))
		err := processor.initialScanPos.CommitTopic()
		if err != nil {
			return nil, fmt.Errorf("%w; Unable to commit topic fot hb", err)
		}
	}

	return &ReplicationStats{requestStats.ModificationsCount,
		0,
		commitDuration,
		requestStats.RequestSize,
		quorumWaitingDuration}, nil
}

func (processor *Processor) FormatTx(ctx context.Context) (*TxBatch, int64, error) {
	quorumWaitingDuration := time.Now().UnixMilli()
	hb, err := processor.waitHbQuorum(ctx)
	quorumWaitingDuration = time.Now().UnixMilli() - quorumWaitingDuration
	if err != nil {
		return nil, 0, err
	}

	// Here we have heartbeat and filled TxQueue - ready to format TX
	xlog.Debug(ctx, "Trying to pop tx until", zap.Any("step", hb.Step))
	txs := processor.txQueue.PopTxsByStep(hb.Step)
	processor.hbTracker.Commit(hb)
	for _, data := range txs {
		xlog.Debug(ctx, "Parsed tx data",
			zap.Any("column_values", data.ColumnValues),
			zap.String("operation_type", data.OperationType.String()),
			zap.Any("key", data.KeyValues),
			zap.Uint64("step", data.Step),
			zap.Uint64("tx_id", data.TxId),
			zap.Uint32("tableId:", data.TableId))
	}
	return &TxBatch{TxData: txs, Hb: hb}, quorumWaitingDuration, nil
}

func (processor *Processor) PushAsSingleTx(ctx context.Context, data dst_table.PushQuery, tx table.Transaction, position types.Position, stage string) error {
	stateParam := table.NewQueryParameters(
		table.ValueParam("stage", ydbTypes.UTF8Value(stage)),
		table.ValueParam("$instanceId", ydbTypes.UTF8Value(processor.instanceId)),
		table.ValueParam("$stateStepId", ydbTypes.Uint64Value(position.Step)),
		table.ValueParam("$stateTxId", ydbTypes.Uint64Value(position.TxId)),
	)
	param := append(data.Parameters, *stateParam...)

	_, err := tx.Execute(ctx, data.Query+processor.stateStoreQuery, &param, options.WithCommit())
	return err
}

func (processor *Processor) PushTxs(ctx context.Context, data dst_table.PushQuery, tx table.Transaction) error {
	_, err := tx.Execute(ctx, data.Query, &data.Parameters, options.WithCommit())
	return err
}

func (processor *Processor) DoReplication(ctx context.Context, dstTables []*dst_table.DstTable,
	lockExecutor func(fn func(context.Context, table.Session, table.Transaction) error) error) (*ReplicationStats, error) {
	if processor.stage == STAGE_INITIAL_SCAN {
		return processor.DoInitialScan(ctx, dstTables, lockExecutor)
	}

	batch, quorumWaitingDuration, err := processor.FormatTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w; Unable to format tx for destination", err)
	}

	query, requestStats, err := processor.assignTxsToDstTables(ctx, batch.TxData, dstTables)
	if err != nil {
		return nil, err
	}

	commitDuration := time.Now().UnixMilli()

	err = lockExecutor(func(ctx context.Context, ts table.Session, txr table.Transaction) error {
		return processor.PushAsSingleTx(ctx, query, txr, types.Position{Step: batch.Hb.Step, TxId: 0}, STAGE_RUN)
	})

	commitDuration = time.Now().UnixMilli() - commitDuration

	if err != nil {
		return nil, fmt.Errorf("%w; Unable to push tx", err)
	}

	processor.lastStep.Store(batch.Hb.Step)

	for i := 0; i < len(batch.TxData); i++ {
		err := batch.TxData[i].CommitTopic()
		if err != nil {
			return nil, fmt.Errorf("%w; Unable to commit topic fot dataTx", err)
		}
	}

	xlog.Debug(ctx, "commit hb in topic", zap.Uint64("step", batch.Hb.Step))
	err = batch.Hb.CommitTopic()
	if err != nil {
		return nil, fmt.Errorf("%w; Unable to commit topic fot hb", err)
	}
	return &ReplicationStats{requestStats.ModificationsCount,
		batch.Hb.Step,
		commitDuration,
		requestStats.RequestSize,
		quorumWaitingDuration}, nil
}
