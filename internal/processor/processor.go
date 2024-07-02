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
	"time"
)

type Processor struct {
	txChannel       chan func() error
	hbTracker       *hb_tracker.HeartBeatTracker
	txQueue         *tx_queue.TxQueue
	dstServerClient table.Client
	lastStep        uint64
	stateStoreQuery string
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
	p.txChannel = make(chan func() error, 100)
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
	p.lastStep = *step
	xlog.Debug(ctx, "processor created", zap.Uint64("last step", p.lastStep))
	return &p, err
}

func (processor *Processor) EnqueueHb(ctx context.Context, hb types.HbData) {
	// Skip all before we already processed
	if hb.Step < processor.lastStep {
		_ = hb.CommitTopic()
		xlog.Debug(ctx, "skip old hb", zap.Uint64("step", hb.Step))
		return
	}
	processor.txChannel <- func() error {
		return processor.hbTracker.AddHb(hb)
	}
}

func (processor *Processor) EnqueueTx(ctx context.Context, tx types.TxData) {
	// Skip all before we already processed
	if tx.Step < processor.lastStep {
		_ = tx.CommitTopic()
		xlog.Debug(ctx, "skip old tx", zap.Uint64("step", tx.Step))
		return
	}
	processor.txChannel <- func() error {
		processor.txQueue.PushTx(tx)
		return nil
	}
}

func (processor *Processor) FormatTx(ctx context.Context) (*TxBatch, error) {
	var hb types.HbData
	for {
		var maxEventPerIteration int = 100
		for maxEventPerIteration > 0 {
			maxEventPerIteration--
			select {
			case fn := <-processor.txChannel:
				err := fn()
				if err != nil {
					xlog.Debug(ctx, "Unable to push event")
					return nil, err
				}
			default:
				maxEventPerIteration = 0
			}
		}
		var ok bool
		hb, ok = processor.hbTracker.GetReady()
		if ok {
			xlog.Debug(ctx, "Got ready hb ", zap.Any("step", hb.Step))
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

func (processor *Processor) DoReplication(ctx context.Context, dstTables []*dst_table.DstTable, dstDb table.Client) error {
	txDataPerTable := make([][]types.TxData, len(dstTables))
	batch, err := processor.FormatTx(ctx)
	if err != nil {
		return fmt.Errorf("%w; Unable to format tx for destination", err)
	}
	for i := 0; i < len(batch.TxData); i++ {
		txDataPerTable[batch.TxData[i].TableId] = append(txDataPerTable[batch.TxData[i].TableId], batch.TxData[i])
	}
	if len(txDataPerTable) != len(dstTables) {
		return fmt.Errorf("Size of dstTables and tables in the tx mismatched",
			zap.Int("txDataPertabe", len(txDataPerTable)),
			zap.Int("dstTable", len(dstTables)))
	}
	var query dst_table.PushQuery
	for i := 0; i < len(txDataPerTable); i++ {
		q, err := dstTables[i].GenQuery(ctx, txDataPerTable[i], i)
		if err != nil {
			return fmt.Errorf("%w; Unable to generate query", err)
		}
		query.Query += q.Query
		query.Parameters = append(query.Parameters, q.Parameters...)

	}
	xlog.Debug(ctx, "Query to perform", zap.String("query", query.Query))
	err = processor.PushAsSingleTx(ctx, dstDb, query, types.Position{Step: batch.Hb.Step, TxId: 0})
	if err != nil {
		return fmt.Errorf("%w; Unable to push tx", err)
	}
	for i := 0; i < len(batch.TxData); i++ {
		err := batch.TxData[i].CommitTopic()
		if err != nil {
			return fmt.Errorf("%w; Unable to commit topic fot dataTx", err)
		}
	}
	xlog.Debug(ctx, "commit hb in topic")
	err = batch.Hb.CommitTopic()
	if err != nil {
		return fmt.Errorf("%w; Unable to commit topic fot Hb", err)
	}
	return nil
}
