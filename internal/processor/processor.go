package processor

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/tx_queue"
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"go.uber.org/zap"
	"time"
)

type Processor struct {
	txChannel chan func() error
	hbTracker *hb_tracker.HeartBeatTracker
	txQueue   *tx_queue.TxQueue
}

type Channel interface {
	EnqueueTx(ctx context.Context, data types.TxData)
	EnqueueHb(ctx context.Context, heartbeat types.HbData)
}

type TxBatch struct {
	TxData []types.TxData
	Hb     types.HbData
}

func NewProcessor(total int) *Processor {
	var p Processor
	p.hbTracker = hb_tracker.NewHeartBeatTracker(total)
	p.txChannel = make(chan func() error)
	p.txQueue = tx_queue.NewTxQueue()
	return &p
}

func (processor *Processor) EnqueueHb(ctx context.Context, hb types.HbData) {
	processor.txChannel <- func() error {
		return processor.hbTracker.AddHb(hb)
	}
}

func (processor *Processor) EnqueueTx(ctx context.Context, tx types.TxData) {
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
			zap.Uint64("tx_id", data.TxId))
	}
	return &TxBatch{TxData: txs, Hb: hb}, nil
}
