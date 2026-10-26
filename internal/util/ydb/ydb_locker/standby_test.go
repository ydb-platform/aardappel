package ydb_locker

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

func (suite *LockerTestSuite) TestStandbyDoesNotInvalidateOwnerTransaction() {
	t := suite.T()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := ConnectToDb(t, suite.LocalYdb, ctx)
	defer db.Close(ctx)
	builder := GetDefaultRequestBuilder("standby_" + uuid.New().String()[:8])
	require.NoError(t, CreateLocksTable(ctx, db.Scripting(), builder))
	defer DropTableIfExists(t, context.Background(), db.Scripting(), builder.TableName)
	// Feed initialization inserts the state row with null lock columns.
	err := db.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		_, res, err := session.Execute(ctx, table.DefaultTxControl(),
			fmt.Sprintf("INSERT INTO %s (lock_name) VALUES ('lock1'u)", builder.TableName), nil)
		if err != nil {
			return err
		}
		return res.Close()
	})
	require.NoError(t, err)
	owner, deadline, err := TryLock(ctx, db.Table(), "lock1", "active", time.Minute, builder)
	require.NoError(t, err)
	require.Equal(t, "active", owner)

	err = db.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		ok, tx, err := CheckLockOwner(ctx, session, "lock1", "active", builder)
		require.NoError(t, err)
		require.True(t, ok)
		defer tx.Rollback(ctx)

		owner, standbyDeadline, err := TryLock(ctx, db.Table(), "lock1", "standby", time.Minute, builder)
		require.NoError(t, err)
		require.Equal(t, "active", owner)
		require.Equal(t, deadline, standbyDeadline)

		// Make this a writing transaction, then commit without hiding an
		// invalidation behind the SDK's transaction retry mechanism.
		query, params := builder.GetCreateLockQueryWithParams("written-by-active")
		res, err := tx.Execute(ctx, query, params)
		require.NoError(t, err)
		require.NoError(t, res.Close())
		_, err = tx.CommitTx(ctx)
		require.NoError(t, err, "standby attempt must not invalidate the active transaction")
		return nil
	})
	require.NoError(t, err)

	// Renewal and takeover after release must still work.
	_, renewed, err := TryLock(ctx, db.Table(), "lock1", "active", 2*time.Minute, builder)
	require.NoError(t, err)
	require.True(t, renewed.After(deadline))
	_, _, err = TryLock(ctx, db.Table(), "lock1", "active", 0, builder)
	require.NoError(t, err)
	owner, _, err = TryLock(ctx, db.Table(), "lock1", "standby", time.Minute, builder)
	require.NoError(t, err)
	require.Equal(t, "standby", owner)
}
