package processor

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"time"

	"go.uber.org/zap"
)

type offsetCommitter interface {
	CommitOffset(ctx context.Context, partitionID int64, offset int64) error
}

type offsetCommitTask struct {
	stream types.ElementaryStreamId
	offset int64
	err    error
}

func runOffsetCommitter(ctx context.Context, tasks <-chan offsetCommitTask, readers []offsetCommitter, maxConcurrentOffsetCommits int) {
	latestTasks := make(map[types.ElementaryStreamId]offsetCommitTask)
	scheduled := make(map[types.ElementaryStreamId]bool)
	var activeCommits int
	var queue []types.ElementaryStreamId
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	commitResults := make(chan offsetCommitTask, maxConcurrentOffsetCommits)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for stream := range latestTasks {
				if scheduled[stream] {
					continue
				}
				scheduled[stream] = true
				queue = append(queue, stream)
			}
		case task, ok := <-tasks:
			if !ok {
				return
			}
			if prev, ok := latestTasks[task.stream]; !ok || task.offset > prev.offset {
				latestTasks[task.stream] = task
			}
		case result := <-commitResults:
			delete(scheduled, result.stream)
			activeCommits--
			if result.err != nil {
				xlog.Error(ctx, "unable to commit topic offset",
					zap.Uint32("reader_id", result.stream.ReaderId),
					zap.Int64("partition_id", result.stream.PartitionId),
					zap.Int64("offset", result.offset),
					zap.Error(result.err))
			} else if latestTasks[result.stream].offset <= result.offset {
				delete(latestTasks, result.stream)
			}
		}
		for len(queue) > 0 && activeCommits < maxConcurrentOffsetCommits {
			stream := queue[0]
			queue = queue[1:]
			activeCommits++
			go func(task offsetCommitTask) {
				task.err = readers[task.stream.ReaderId].CommitOffset(ctx, task.stream.PartitionId, task.offset)
				select {
				case commitResults <- task:
				case <-ctx.Done():
				}
			}(latestTasks[stream])
		}
	}
}
