package ydb_locker

import (
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type LockRequestBuilder interface {
	GetLockNameColumnName() string
	GetOwnerColumnName() string
	GetDeadlineColumnName() string

	GetSelectLockQueryWithParams(lockName string) (string, *table.QueryParameters)
	GetUpdateLockQueryWithParams(lockName string, owner string, ttl time.Duration) (string, *table.QueryParameters)
	GetCreateLockQueryWithParams(lockName string) (string, *table.QueryParameters)
}

type LockSchemaRequestBuilder interface {
	GetCreateLocksTableQuery() string
}

type LockRequestBuilderImpl struct {
	TableName          string
	LockNameColumnName string
	OwnerColumnName    string
	DeadlineColumnName string
}

func (l *LockRequestBuilderImpl) GetLockNameColumnName() string {
	return l.LockNameColumnName
}

func (l *LockRequestBuilderImpl) GetOwnerColumnName() string {
	return l.OwnerColumnName
}

func (l *LockRequestBuilderImpl) GetDeadlineColumnName() string {
	return l.DeadlineColumnName
}

func (l *LockRequestBuilderImpl) GetSelectLockQueryWithParams(lockName string) (string, *table.QueryParameters) {
	return fmt.Sprintf(
			`DECLARE $LOCK_NAME AS Utf8;
			SELECT %[3]s FROM %[1]s WHERE %[2]s = $LOCK_NAME`,
			l.TableName, l.LockNameColumnName, l.OwnerColumnName, l.DeadlineColumnName),
		table.NewQueryParameters(table.ValueParam("$LOCK_NAME", types.UTF8Value(lockName)))
}

func (l *LockRequestBuilderImpl) GetUpdateLockQueryWithParams(lockName string, owner string, ttl time.Duration) (string, *table.QueryParameters) {
	// Only write when renewing our lease or acquiring an expired lease.
	return fmt.Sprintf(
			`DECLARE $LOCK_NAME AS Utf8;
			DECLARE $OWNER AS Utf8;
			DECLARE $TTL AS Interval;

			$ts = CurrentUtcTimestamp();
			$new_ts = $ts + $TTL;
			
			update %[1]s
			set %[3]s = $OWNER, %[4]s = $new_ts
			where %[2]s == $LOCK_NAME
				and (%[3]s == $OWNER or $ts >= (%[4]s ?? $ts));

			select %[3]s, %[4]s
			from %[1]s
			where %[2]s == $LOCK_NAME;
		`, l.TableName, l.LockNameColumnName, l.OwnerColumnName, l.DeadlineColumnName),
		table.NewQueryParameters(
			table.ValueParam("$LOCK_NAME", types.UTF8Value(lockName)),
			table.ValueParam("$OWNER", types.UTF8Value(owner)),
			table.ValueParam("$TTL", types.IntervalValueFromMicroseconds(ttl.Microseconds())),
		)
}

func (l *LockRequestBuilderImpl) GetCreateLockQueryWithParams(lockName string) (string, *table.QueryParameters) {
	return fmt.Sprintf(
			`DECLARE $LOCK_NAME AS Utf8;
			INSERT INTO %[1]s
			(%[2]s, %[3]s, %[4]s)
			VALUES ($LOCK_NAME, '', CurrentUtcTimeStamp());`,
			l.TableName, l.LockNameColumnName, l.OwnerColumnName, l.DeadlineColumnName),
		table.NewQueryParameters(table.ValueParam("$LOCK_NAME", types.UTF8Value(lockName)))
}

func (l *LockRequestBuilderImpl) GetCreateLocksTableQuery() string {
	return fmt.Sprintf(`
		create table if not exists %[1]s (
			%[2]s utf8,
			%[3]s utf8,
			%[4]s timestamp,
			primary key (%[2]s)
		);
	`, "`"+l.TableName+"`", l.LockNameColumnName, l.OwnerColumnName, l.DeadlineColumnName)
}

func GetDefaultRequestBuilder(tableName string) *LockRequestBuilderImpl {
	return &LockRequestBuilderImpl{
		TableName:          tableName,
		LockNameColumnName: "lock_name",
		OwnerColumnName:    "owner",
		DeadlineColumnName: "deadline",
	}
}
