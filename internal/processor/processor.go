package processor

import (
	"aardappel/internal/dst_table"
	"aardappel/internal/hb_tracker"
	"aardappel/internal/tx_queue"
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydbTypes "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

type Processor struct {
	txChannel       chan func() error
	hbTracker       *hb_tracker.HeartBeatTracker
	txQueue         *tx_queue.TxQueue
	dstServerClient table.Client
	lastStep        atomic.Uint64
	stateStoreQuery string
}

type ReplicationStats struct {
	ModificationsCount int
	LastHeartBeat      uint64
	CommitDurationMs   int64
}

type Channel interface {
	EnqueueTx(ctx context.Context, data types.TxData)
	EnqueueHb(ctx context.Context, heartbeat types.HbData)
}

type TxBatch struct {
	TxData []types.TxData
	Hb     types.HbData
}

func createStateStoreQuery(stateTable string) string {
	return fmt.Sprintf(`
UPSERT INTO
    %v
    (id, step_id, tx_id)
VALUES
    (0, $stateStepId, $stateTxId);
`, stateTable)
}

func NewProcessor(ctx context.Context, total int, stateTable string, client table.Client) (*Processor, error) {
	var p Processor
	p.hbTracker = hb_tracker.NewHeartBeatTracker(total)
	p.txChannel = make(chan func() error, 1000)
	p.txQueue = tx_queue.NewTxQueue()
	p.dstServerClient = client
	p.stateStoreQuery = createStateStoreQuery(stateTable)

	stateQuery := fmt.Sprintf("SELECT step_id FROM %v WHERE id = 0;", stateTable)
	var step *uint64
	err := p.dstServerClient.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			res, err := tx.Execute(ctx, stateQuery, nil)
			if err != nil {
				return err
			}
			res.NextResultSet(ctx)
			res.NextRow()
			return res.ScanNamed(
				named.Optional("step_id", &step),
			)
		})
	if err != nil {
		return nil, err
	}
	p.lastStep.Store(*step)
	xlog.Debug(ctx, "processor created", zap.Uint64("last step", *step))
	return &p, err
}

func (processor *Processor) StartHbGuard(ctx context.Context, timeout uint32, streamDbgInfos []string) {
	processor.hbTracker.StartHbGuard(ctx, timeout, streamDbgInfos)
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
	processor.txChannel <- func() error {
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
	processor.txChannel <- func() error {
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
}

func (processor *Processor) FormatTx(ctx context.Context) (*TxBatch, error) {
	var hb types.HbData
	for {
		var maxEventPerIteration int = 1000
		for maxEventPerIteration > 0 {
			maxEventPerIteration--
			select {
			case fn := <-processor.txChannel:
				err := fn()
				if err != nil {
					xlog.Debug(ctx, "Unable to push event", zap.Error(err))
					return nil, err
				}
			default:
				maxEventPerIteration = 0
			}
		}
		var ok bool
		hb, ok = processor.hbTracker.GetReady()
		if ok {
			xlog.Debug(ctx, "Got hb quorum", zap.Any("step", hb.Step))
			break
		}
		//TODO: Wait any hb instead of sleep here
		time.Sleep(1 * time.Millisecond)
	}
	// Here we have heartbeat and filled TxQueue - ready to format TX
	xlog.Debug(ctx, "Trying to pop tx until", zap.Any("step", hb.Step))
	txs := processor.txQueue.PopTxs(hb.Step)
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
	return &TxBatch{TxData: txs, Hb: hb}, nil
}

func (processor *Processor) PushAsSingleTx(ctx context.Context, client table.Client, data dst_table.PushQuery, position types.Position) error {
	stateParam := table.NewQueryParameters(
		table.ValueParam("$stateStepId", ydbTypes.Uint64Value(position.Step)),
		table.ValueParam("$stateTxId", ydbTypes.Uint64Value(position.TxId)),
	)
	param := append(data.Parameters, *stateParam...)

	return client.DoTx(ctx,
		func(ctx context.Context, tx table.TransactionActor) error {
			_, err := tx.Execute(ctx, data.Query+processor.stateStoreQuery, &param)
			return err
		})
}

func (processor *Processor) DoReplication(ctx context.Context, dstTables []*dst_table.DstTable, dstDb table.Client) (*ReplicationStats, error) {
	txDataPerTable := make([][]types.TxData, len(dstTables))
	batch, err := processor.FormatTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w; Unable to format tx for destination", err)
	}
	for i := 0; i < len(batch.TxData); i++ {
		txDataPerTable[batch.TxData[i].TableId] = append(txDataPerTable[batch.TxData[i].TableId], batch.TxData[i])
	}
	if len(txDataPerTable) != len(dstTables) {
		return nil, fmt.Errorf("Size of dstTables and tables in the tx mismatched",
			zap.Int("txDataPertabe", len(txDataPerTable)),
			zap.Int("dstTable", len(dstTables)))
	}
	var query dst_table.PushQuery
	var modifications int
	for i := 0; i < len(txDataPerTable); i++ {
		q, err := dstTables[i].GenQuery(ctx, txDataPerTable[i], i)
		if err != nil {
			return nil, fmt.Errorf("%w; Unable to generate query", err)
		}
		query.Query += q.Query
		modifications += q.ModificationsCount
		query.Parameters = append(query.Parameters, q.Parameters...)

	}
	xlog.Debug(ctx, "Query to perform", zap.String("query", query.Query))
	commitDuration := time.Now().UnixMilli()
	err = processor.PushAsSingleTx(ctx, dstDb, query, types.Position{Step: batch.Hb.Step, TxId: 0})
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
	return &ReplicationStats{modifications,
		batch.Hb.Step,
		commitDuration}, nil
}
