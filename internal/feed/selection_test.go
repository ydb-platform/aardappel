package feed

import (
	"context"
	"testing"
	"time"

	"aardappel/internal/config"
	"aardappel/internal/util/ydb/ydb_locker"
)

func selectionCandidates(t *testing.T, storage *ydb_locker.LocalLockStorage, owner string) []Candidate {
	t.Helper()
	var candidates []Candidate
	for _, id := range []string{"first", "second", "third"} {
		if _, err := storage.CreateLock(context.Background(), id); err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, Candidate{
			Feed:   config.Feed{ID: id},
			Locker: ydb_locker.NewLocker(storage, id, owner, time.Minute),
		})
	}
	return candidates
}

func TestFeedSelectionWorkersClaimDifferentFeeds(t *testing.T) {
	storage := ydb_locker.NewLocalLockStorage()
	selectedIDs := make(map[string]bool)
	for _, owner := range []string{"worker-a", "worker-b"} {
		candidates := selectionCandidates(t, storage, owner)
		selected, ok := SelectAnyFeed(context.Background(), candidates, false, time.Millisecond)
		if !ok || selectedIDs[selected.Feed.ID] {
			t.Fatalf("worker %s selected %q, acquired %v", owner, selected.Feed.ID, ok)
		}
		selectedIDs[selected.Feed.ID] = true
	}
	claimed := 0
	for _, lock := range storage.Locks {
		if lock.OwnerName != "" {
			claimed++
		}
	}
	if claimed != 2 {
		t.Fatalf("claimed %d feeds, want 2", claimed)
	}
}

func TestFeedSelectionIdleTimeout(t *testing.T) {
	storage := ydb_locker.NewLocalLockStorage()
	candidates := selectionCandidates(t, storage, "worker")
	for _, candidate := range candidates {
		if _, _, err := storage.TryLock(context.Background(), candidate.Feed.ID, "other", time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := SelectAnyFeed(context.Background(), candidates, false, time.Millisecond); ok {
		t.Fatal("acquired an occupied feed")
	}
}

func TestFeedSelectionWaitsForAvailableFeed(t *testing.T) {
	storage := ydb_locker.NewLocalLockStorage()
	candidates := selectionCandidates(t, storage, "worker")[:1]
	if _, _, err := storage.TryLock(context.Background(), "first", "other", 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if selected, ok := SelectAnyFeed(ctx, candidates, true, time.Millisecond); !ok || selected.Feed.ID != "first" {
		t.Fatal("did not acquire feed after its lease expired")
	}
}

func TestFeedSelectionCancellation(t *testing.T) {
	storage := ydb_locker.NewLocalLockStorage()
	candidates := selectionCandidates(t, storage, "worker")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := SelectAnyFeed(ctx, candidates, true, time.Millisecond); ok {
		t.Fatal("acquired feed after cancellation")
	}
}
