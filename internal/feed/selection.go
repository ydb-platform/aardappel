package feed

import (
	"context"
	"math/rand"
	"time"

	"aardappel/internal/config"
	"aardappel/internal/util/xlog"
	"aardappel/internal/util/ydb/ydb_locker"

	"go.uber.org/zap"
)

type Candidate struct {
	Feed   config.Feed
	Locker *ydb_locker.Locker
}

// SelectAnyFeed scans candidates in randomized order and claims at most one feed. Only the selected candidate starts a
// background locker, so idle workers cannot reserve additional feeds.
func SelectAnyFeed(ctx context.Context, candidates []Candidate, wait bool, interval time.Duration) (Candidate, bool) {

	randomIndices := rand.Perm(len(candidates))
	timer := time.NewTicker(interval)
	defer timer.Stop()
	checks := 0
	for {
		for _, i := range randomIndices {
			candidate := candidates[i]
			if ctx.Err() != nil {
				return Candidate{}, false
			}
			locker := candidate.Locker
			owner, _, err := locker.LockStorage.TryLock(ctx, locker.LockName, locker.OwnerName, locker.Ttl)
			if err != nil {
				xlog.Error(ctx, "Feed lock acquisition attempt failed; continuing feed selection", zap.String("feed_id", candidate.Feed.ID), zap.String("cluster_id", candidate.Feed.Destination), zap.Error(err))
				continue
			}
			if owner == locker.OwnerName {
				return candidate, true
			}
		}
		select {
		case <-ctx.Done():
			return Candidate{}, false
		case <-timer.C:
			checks++
			if !wait && checks >= 10 {
				xlog.Info(ctx, "No feed lock acquired within the startup wait limit; exiting worker", zap.Int("candidate_count", len(candidates)), zap.Int("wait_intervals", checks), zap.Duration("check_interval", interval))
				return Candidate{}, false
			}
			xlog.Info(ctx, "No feed lock acquired; retrying feed selection", zap.Int("candidate_count", len(candidates)), zap.Duration("check_interval", interval))
		}
	}
}
