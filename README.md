# Aardappel

[English](#aardappel) · [Русский](#aardappel-русский)

Aardappel is an asynchronous CDC replication service for YDB databases. It reads
changefeed topics from a source database, restores the global order of changes by
virtual timestamp, and atomically applies a consistent set of changes to tables in
the destination database.

Its main property is that changes from all configured streams that have reached a
common heartbeat boundary are written to the destination tables together with the
new replication position in a single YDB transaction. Topic message offsets are
committed only after that transaction has committed successfully.

## Contents

- [Architecture](#architecture)
- [Replication algorithm](#replication-algorithm)
- [Startup and recovery](#startup-and-recovery)
- [Preparing YDB](#preparing-ydb)
- [Configuration](#configuration)
- [Adding a new stream](#adding-a-new-stream)
- [Authentication](#authentication)
- [Problem message handling](#problem-message-handling)
- [Key filtering](#key-filtering)
- [Monitoring](#monitoring)
- [Build and run](#build-and-run)
- [Limitations and operational notes](#limitations-and-operational-notes)

## Architecture

```mermaid
flowchart LR
    subgraph SRC[Source YDB]
        T1[Source table 1] --> CDC1[CDC 1]
        T2[Source table 2] --> CDC2[CDC 2]

        subgraph TOPIC1[CDC topic 1]
            C1P0[Partition 0]
            C1P1[Partition 1]
            C1PN[Partition N]
        end

        subgraph TOPIC2[CDC topic 2]
            C2P0[Partition 0]
            C2P1[Partition 1]
        end

        CDC1 --> C1P0
        CDC1 --> C1P1
        CDC1 --> C1PN
        CDC2 --> C2P0
        CDC2 --> C2P1
    end

    subgraph APP[Aardappel]
        subgraph RS[Topic read sessions<br/>one session per stream]
            R1[Partition reader<br/>CDC 1 / partition 0]
            R2[Partition reader<br/>CDC 1 / partition 1]
            RN[Partition reader<br/>CDC 1 / partition N]
            R3[Partition reader<br/>CDC 2 / partition 0]
            R4[Partition reader<br/>CDC 2 / partition 1]
        end
        P[Parser + ordered TxQueue]
        H[Heartbeat tracker]
        B[Batch/query builder]
        L[Distributed lock]
    end

    subgraph DST[Destination YDB]
        D1[Destination table 1]
        D2[Destination table N]
        S[State/lock table]
        KF[Optional key filter table]
        CQ[Optional command topic]
        DLQ[Optional dead-letter topic]
    end

    C1P0 --> R1
    C1P1 --> R2
    C1PN --> RN
    C2P0 --> R3
    C2P1 --> R4
    R1 & R2 & RN & R3 & R4 -->|update / erase| P
    R1 & R2 & RN & R3 & R4 -->|resolved timestamp| H
    P --> B
    H -->|quorum boundary| B
    CQ -. skip / apply .-> RS
    KF -. blocked keys .-> B
    B --> L
    L -->|one YDB transaction| D1
    L -->|one YDB transaction| D2
    L -->|position + stage| S
    RS -. problem details .-> DLQ
```

A CDC changefeed is represented by a YDB topic and may contain multiple
partitions. Each `streams` entry creates one topic read session, while the YDB SDK
assigns reading of individual partitions inside that session. Therefore, every
partition has its own logical **partition reader** in the diagram. The replication
algorithm treats it as an elementary stream identified by
`(stream_id, partition_id)`.

Main components:

- `reader` creates a topic read session for every `streams` entry; messages from
  every assigned partition are handled as a separate stream, parsed from CDC JSON,
  and sent to `Processor`;
- `TxQueue` merges changes from all tables and orders them by `(step, tx_id)`;
- `HeartBeatTracker` stores the latest resolved timestamp for every partition of
  every stream;
- `Processor` selects a consistent batch, builds YQL for every destination table,
  and manages the replication position;
- `DstTable` reads the destination table schema and converts CDC values to its YDB
  types;
- `Locker` prevents two processes with the same `instance_id` from writing the
  replica concurrently.

## Replication algorithm

### Position and heartbeat quorum

CDC messages must contain virtual timestamps. A message position is the
lexicographically ordered pair:

```text
(step_a, tx_id_a) < (step_b, tx_id_b) when
step_a < step_b or (step_a == step_b and tx_id_a < tx_id_b)
```

A resolved timestamp arrives independently from the partition reader of each
partition. One CDC may have several partitions, and every partition independently
participates in calculating the common boundary. A quorum is ready only when a
heartbeat has been received from **every partition of every configured CDC
stream**. The safe boundary is the minimum of those heartbeats:

```text
quorum = min(last_resolved[stream, partition])
```

Every change whose position is strictly lower than the quorum could already have
arrived from every partition and can be applied in global order. A change equal to
the quorum is included in a later batch.

```mermaid
sequenceDiagram
    participant YT as Source topics
    participant R as Readers
    participant Q as TxQueue
    participant H as HeartbeatTracker
    participant P as Processor
    participant YD as Destination YDB

    YT->>R: update/erase + ts(step, tx_id)
    R->>Q: enqueue change
    YT->>R: resolved timestamp per partition
    R->>H: enqueue heartbeat
    P->>H: wait until every partition has heartbeat
    H-->>P: min heartbeat = quorum
    P->>Q: pop changes with ts < quorum
    P->>YD: UPSERT/DELETE + state position (single transaction)
    YD-->>P: commit OK
    P->>R: commit change offsets
    P->>R: commit quorum heartbeat offset
```

### Query construction

For one quorum, changes are distributed by `TableId`, which is the current index
of the entry in `streams`. Within each table:

- multiple updates to one key are merged into the final set of columns;
- the last operation for a key determines the final `UPSERT` or `DELETE`;
- UPSERT groups with the same column set are combined through `AS_TABLE`;
- parameter types are obtained from the actual destination table schema.

YQL for all destination tables and the replication position UPSERT into
`state_table` run in one transaction. This preserves atomicity between the tables
and the checkpoint.

### Committing messages

The operation order is essential:

1. Apply changes and update the checkpoint in destination YDB.
2. Successfully commit the YDB transaction.
3. Commit the offsets of CDC change messages.
4. Commit the offset of the quorum heartbeat.

If the process exits between steps 2 and 3, messages may be read again. On restart,
they are discarded using the stored position. Destination writes are therefore
idempotent at the checkpoint and final UPSERT/DELETE level, while topic delivery
is effectively at-least-once.

## Startup and recovery

### State table

The table is created automatically in destination YDB:

```sql
CREATE TABLE IF NOT EXISTS <state_table> (
    id Utf8,
    step_id Uint64,
    tx_id Uint64,
    state Utf8,
    stage Utf8,
    last_msg Utf8,
    lock_owner Utf8,
    lock_deadline Timestamp,
    PRIMARY KEY (id)
);
```

One row belongs to one `instance_id` and stores:

- checkpoint: `step_id`, `tx_id`;
- state: `OK` or `FATAL_ERROR`;
- stage: `INITIAL_SCAN` or `RUN`;
- the last fatal error;
- distributed lock owner and deadline.

On the first launch, a row is created with position `(0, 0)`, state `OK`, and
stage `INITIAL_SCAN`.

### INITIAL_SCAN

The initial stage establishes a stable common boundary for all streams:

1. Wait for the first complete heartbeat set.
2. Remember the maximum heartbeat in that set.
3. Wait for a quorum strictly greater than that maximum.
4. Apply accumulated changes before that position in batches of up to 1000
   messages.
5. Store the position and change the stage to `RUN` in the final transaction.

### RUN

In normal operation, each iteration waits for a quorum, selects all changes
strictly before it, atomically applies them, and stores the quorum as the new
checkpoint.

### Restart

At startup, `Processor` loads the row for its `instance_id`. Messages and
heartbeats older than the stored position are immediately committed without being
applied. If the stored state is not `OK`, automatic continuation is refused: fix
the cause and recover the state deliberately first.

### Multiple instances

Before reading source topics, the process acquires a lock in the row for its
`instance_id`. Lock TTL is `2 × max_expected_heartbeat_interval`.

- `multiple_instances_mode: true`: standby processes keep waiting and may take
  over after the lock is released;
- `multiple_instances_mode: false`: the process stops waiting after a series of
  failed checks, performed every 5 seconds.

Processes for independent replication sets sharing one state table need different
`instance_id` values. Active/standby processes for the same replication use the
same `instance_id` and identical stream configuration.

## Preparing YDB

### Source

Every source table needs a JSON `UPDATES` changefeed with virtual and resolved
timestamps, plus a consumer:

```bash
YDB_EXEC="ydb -e <DATABASE_ENDPOINT> -d <DATABASE_NAME>"

${YDB_EXEC} 'ALTER TABLE `<TABLE_NAME>` ADD CHANGEFEED `<CDC_NAME>` WITH (
  FORMAT = "JSON",
  MODE = "UPDATES",
  VIRTUAL_TIMESTAMPS = TRUE,
  RESOLVED_TIMESTAMPS = Interval("PT1S"),
  INITIAL_SCAN = TRUE
);'

${YDB_EXEC} 'ALTER TOPIC `<TABLE_NAME>/<CDC_NAME>` ADD CONSUMER `<CONSUMER_NAME>`;'
```

Enable `INITIAL_SCAN = TRUE` when the source table already contains data. The
changefeed then exports existing rows first and continues with new changes. It may
be omitted for a table guaranteed to be empty. Resolved timestamps may temporarily
stop during the initial scan, so Aardappel starts forming a heartbeat quorum after
the scan completes.

Every `streams` entry must reference such a topic and consumer. See the
[YDB initial scan documentation](https://ydb.tech/docs/en/yql/reference/syntax/alter_table/changefeed).

### Destination

Destination tables must be created in advance. For every source/destination pair:

- primary key column order and types must be compatible;
- CDC columns must exist in destination and have compatible types;
- the service account needs schema-read and data-write permissions;
- `state_table` must not conflict with an application table.

Aardappel does not require the complete source and destination schemas to be
identical. The practical requirement is that every source CDC message can be
applied to the corresponding destination table: all message columns must exist
and convert to destination types, and the key must match the destination primary
key.

Change schemas before dependent CDC messages appear. For example, add a compatible
column to destination first, then to source, and only then begin writing that
column in source. Otherwise Aardappel may receive a column unknown to its
destination schema and stop replication.

Each destination schema is read once at startup and cached in memory. Restart
Aardappel after a destination schema change, before dependent source messages are
produced. On restart, it runs `DescribeTable` again and refreshes its local schema.

## Configuration

The file is supplied through `-config`; the default is `config.yaml` in the current
directory. See the full example in
[`cmd/aardappel/config.yaml`](cmd/aardappel/config.yaml).

Configuration is read once at startup. There is no automatic reload: changing
connection strings, authentication, streams, queues, monitoring, or any other
setting requires an application restart.

```yaml
src_connection_string: "grpcs://source.example.net:2135/Root/source"
src_client_balancer: true
src_oauth2_file: "/run/secrets/source-oauth.json"
# src_static_token: "..."
# src_oauth2_endpoint: "https://sts.example.net/oauth2/token/exchange"

dst_connection_string: "grpcs://destination.example.net:2135/Root/destination"
dst_client_balancer: true
dst_oauth2_file: "/run/secrets/destination-oauth.json"
# dst_static_token: "..."
# dst_oauth2_endpoint: "https://sts.example.net/oauth2/token/exchange"

state_table: "/Root/aardappel_state"
instance_id: "orders-replica"
multiple_instances_mode: true
max_expected_heartbeat_interval: 10
log_level: "info"

streams:
  - src_topic: "/Root/orders/orders_cdc"
    consumer: "aardappel"
    dst_table: "/Root/orders_replica"
    problem_strategy: "stop"
    mon_tag: "orders"

mon_server:
  listen: ":8080"

# Optional:
# cmd_queue:
#   path: "/Root/aardappel_commands"
#   consumer: "aardappel"
# dead_letter_queue:
#   path: "/Root/aardappel_dlq"
# key_filter:
#   table_path: "/Root/aardappel_blocked_keys"
```

### Top-level settings

| Setting | Required | Purpose |
|---|---:|---|
| `src_connection_string` | yes | Source YDB connection string. |
| `src_client_balancer` | no | `false` enables `SingleConn`, useful when direct node access is unavailable. |
| `src_oauth2_file` / `src_static_token` | yes | Exactly one source authentication method. |
| `src_oauth2_endpoint` | no | Overrides the credentials-file token exchange endpoint. |
| `dst_connection_string` | yes | Destination YDB connection string. |
| `dst_client_balancer` | no | Equivalent destination setting. |
| `dst_oauth2_file` / `dst_static_token` | yes | Exactly one destination authentication method. |
| `dst_oauth2_endpoint` | no | Overrides the destination token exchange endpoint. |
| `state_table` | yes | Destination service table for state and locking. |
| `instance_id` | yes | Replication identifier and state table row key. |
| `multiple_instances_mode` | no | Keeps a standby process waiting for the lock. |
| `streams` | yes | CDC topic to destination table mappings. |
| `max_expected_heartbeat_interval` | recommended | Missing-heartbeat warning threshold and lock TTL input. |
| `log_level` | no | `debug`, `info`, `warn`, or `error`. |
| `mon_server` | no | Enables metrics and readiness HTTP endpoints. |
| `cmd_queue` | no | External `skip`/`apply` decisions for out-of-order messages. |
| `dead_letter_queue` | no | Diagnostic topic for problem transactions. |
| `key_filter` | no | Table of keys whose changes must not be applied. |

### `streams[]` settings

| Setting | Required | Purpose |
|---|---:|---|
| `src_topic` | yes | Source CDC topic path. |
| `consumer` | yes | Consumer in that topic. |
| `dst_table` | yes | Pre-created destination table path. |
| `problem_strategy` | no | `stop` (default) or `continue`. |
| `mon_tag` | no | `stream_tag` label; defaults to `src_topic`. |

A consumer must be exclusive to one concurrently running Aardappel replication
group. The same consumer name is fine in different CDC topics, but independent
instances must not read the same consumer of the same topic.

Stream order is not stored in the state table; only the common
`(step_id, tx_id)` position is stored. After restart, configuration is reread and
`TableId` is reassigned from the current index. Existing entries may therefore be
reordered if every `src_topic → dst_table` pair remains unchanged.

Adding, removing, or remapping a stream requires checkpoint analysis. All streams
share one global position: history in a new stream older than the stored checkpoint
may be treated as already processed and skipped. Such a migration may require a
new `instance_id`, consumer/state, or preliminary data alignment.

## Adding a new stream

A new stream can be added to an existing replication instance, for example when a
new source table must be replicated consistently with existing tables. It joins
the same heartbeat quorum, and its changes are applied in the same destination
transaction as changes from other streams.

The main task when adding a stream is to **align its position with the existing
replication**. There are two independent position layers:

- `step_id` and `tx_id` in `state_table` form one global virtual timestamp for all
  streams of the instance. Aardappel uses it to filter out messages that it
  considers already processed;
- each CDC consumer stores committed offsets separately for every topic partition.
  These offsets determine where physical reading of each stream resumes.

Resetting the global timestamp in `state_table` does not roll back the consumers.
Existing streams therefore continue from their committed offsets and read only new
messages, while the new consumer starts from its own offset, normally from the
beginning. The streams eventually reach a common heartbeat quorum and continue in
the normal consistent mode.

Choose the migration procedure according to the state and size of the new stream:

```mermaid
flowchart TD
    A[Add a new stream] --> B{Is the source table empty?}
    B -->|yes| C[Add the stream to config<br/>and restart Aardappel]
    B -->|no| D{Does its history fit<br/>into one transaction?}
    D -->|yes| E[Stop replication<br/>add stream<br/>reset timestamp to 0]
    D -->|no| F[Stop replication<br/>add stream<br/>reset timestamp to 0<br/>set INITIAL_SCAN]
    E --> G[Start and wait for streams<br/>to align at a common quorum]
    F --> H[Aardappel applies initial scan in batches<br/>and switches automatically to RUN]
```

### Scenario 1: a new empty table and a light stream

If the source table is new and empty and the stream has no history to replay:

1. Create the compatible destination table, CDC, and consumer.
2. Add the `src_topic → dst_table` pair to `streams`.
3. Restart Aardappel so that it reloads the configuration and destination schema.
4. Check that the new stream starts producing heartbeats and joins the common
   quorum.

No `state_table` change is required in this case. The CDC and consumer must be
created before writes to the new source table begin.

### Scenario 2: a non-empty stream that fits into one transaction

Use this procedure when the new table and CDC already contain data, but the entire
history before the first common quorum is small enough for one destination
transaction:

1. Stop the active process and every standby process with the same `instance_id`.
2. Back up the current state row and record committed offsets for every consumer
   partition.
3. Add the new stream to the configuration.
4. Reset the global timestamp for this instance to `(0, 0)` so that Aardappel does
   not filter the new stream's historical messages using the old checkpoint:

   ```sql
   UPDATE `<state_table>`
   SET step_id = 0, tx_id = 0
   WHERE id = "<instance_id>";
   ```

5. Start Aardappel.

The existing consumers retain their committed offsets, so the old streams read
only messages that appeared after their last commits. The new consumer reads its
stream from the beginning and applies its history. At some point all streams align
at a common heartbeat quorum, after which replication continues normally.

### Scenario 3: a large stream created with initial scan

Use this procedure when the new CDC contains an initial scan that is too large for
one destination request or transaction:

1. Stop the active process and every standby process with the same `instance_id`.
2. Back up the current state row and record committed offsets for every consumer
   partition.
3. Add the new stream to the configuration.
4. Reset the global timestamp to `(0, 0)` and switch the instance to
   `INITIAL_SCAN`:

   ```sql
   UPDATE `<state_table>`
   SET step_id = 0,
       tx_id = 0,
       stage = "INITIAL_SCAN"
   WHERE id = "<instance_id>";
   ```

5. Start Aardappel.

In `INITIAL_SCAN`, `Processor` applies changes in batches of no more than 1000 CDC
messages. Once it reaches a synchronized heartbeat quorum, it stores the new global
position, switches `stage` to `RUN` automatically, and resumes normal replication.
The existing streams continue from their committed consumer offsets; the new
stream reads and applies its initial-scan history and catches up with them.

### Alternative: preliminary replication through a separate instance

For a large table, the new stream can first be replicated through a temporary,
separate Aardappel instance:

1. Create a separate `instance_id`, state row, and consumer for the new stream.
2. Replicate the new table until the temporary instance catches up.
3. Stop both the main and temporary replication instances.
4. Add the new stream to the main configuration and perform the alignment from
   Scenario 2: reset the main global timestamp to `(0, 0)` and start the main
   instance.
5. Reuse or align the consumer offset at the confirmed cutover point so that the
   main instance continues from the data already applied by the temporary
   replication.
6. After checking the common quorum, lag, and destination data, remove the
   temporary replication instance.

Never let the temporary and main instances read the same consumer concurrently.

### Transition period and safety

During catch-up or batched initial-scan processing, the newly added destination
table may be only partially populated. The complete set of destination tables may
therefore remain logically inconsistent for some time, even though every
individual batch is applied atomically. Consumers should avoid using the new table
until alignment completes or explicitly support this transitional state.

Manual changes to `state_table` and consumer offsets carry data-loss and duplicate
application risks. Perform them only while all relevant readers are stopped. Keep
a copy of the original state and offsets, verify that `state = "OK"`, and confirm
completion using heartbeat/quorum, replication lag, logs, and an application-level
data comparison.

> **Do not concurrently use one consumer of one CDC topic from independent
> Aardappel instances.** YDB distributes partitions between read sessions: one
> process receives one subset and another receives the rest. Each
> `HeartBeatTracker` expects all partitions, so neither process can form its
> expected full quorum correctly. Create separate consumers for independent
> instances. Active/standby processes with the same `instance_id` may share a
> consumer because the distributed lock permits only one process to read.

## Authentication

Source and destination authentication are configured independently. Set
**exactly one** method for each side:

- `*_static_token`: a final YDB access token without OAuth2 exchange;
- `*_oauth2_file`: a JSON file describing OAuth2 token exchange.

Two credential-file formats are supported:

1. Native YDB Go SDK OAuth2 format (`token-endpoint`, `subject-credentials`, and
   SDK-supported token sources).
2. `oauth2_token_exchange`, where actor/subject tokens may come from separate
   files or inline values. This is convenient for projected Kubernetes service
   account tokens.

`src_oauth2_endpoint` and `dst_oauth2_endpoint` override the endpoint in their
respective credentials file. See [`internal/auth/README.md`](internal/auth/README.md)
for complete field descriptions and JSON examples.

## Problem message handling

Each reader tracks the latest heartbeat of each partition. A change arriving with
a lower position violates the expected stream order.

```mermaid
flowchart TD
    O[Out-of-order change received] --> C{Is cmd_queue configured?}
    C -->|yes| F{Command found for<br/>instance + topic + key + ts?}
    F -->|apply| A[Apply change]
    F -->|skip| S[Skip and commit offset]
    F -->|no| P{problem_strategy}
    C -->|no| P
    P -->|continue| D[Write details to DLQ<br/>if configured]
    D --> S
    P -->|stop| E[Store FATAL_ERROR]
    E --> SD[Write problem to DLQ<br/>if configured]
    SD --> R[Read until the next heartbeat;<br/>write other out-of-order<br/>transactions to DLQ]
    R --> X[Stop the process]
```

A `cmd_queue` command looks like this:

```json
{
  "aardapel_instance_id": "orders-replica",
  "path": "/Root/orders/orders_cdc",
  "key": ["order-42"],
  "ts": [123456789, 17],
  "action": "skip"
}
```

Valid actions are `skip` and `apply`. The spelling `aardapel_instance_id` is
intentional because that is what the current JSON parser expects.

`dead_letter_queue` receives a textual diagnostic for events that were skipped or
stopped replication. With the `stop` strategy, Aardappel writes the original
problem to DLQ, then reads the stream up to the next heartbeat and writes any other
out-of-order transactions found in that interval. These messages are written only
when DLQ is configured. The problem message is not committed by this path; after
collecting diagnostics, the process stops. DLQ is not an automatic replay source.

## Key filtering

`key_filter.table_path` connects a blocked-key table. At startup, keys for the
current `instance_id` are loaded into memory; matching changes are excluded from
the destination transaction YQL.

Operators create the filter table in advance. It must support range reads by
`instance_id` and writes to these fields:

```sql
CREATE TABLE `/Root/aardappel_blocked_keys` (
    instance_id Utf8,
    key String,
    PRIMARY KEY (instance_id, key)
);
```

`key` is a binary serialization of the primary key together with the destination
table path. Do not construct it manually without the same serializer.

## Monitoring

When `mon_server.listen` is configured, the service exposes:

- `GET /metrics`: Prometheus metrics;
- `GET /readyz`: `200` after source topics and destination tables have been
  checked successfully, otherwise `503`.

Multiple instances on one host need different listen ports.

| Metric | Type | Meaning |
|---|---|---|
| `modifications_count` | counter | Applied modifications. |
| `modifications_count_per_table{stream_tag}` | counter | Modifications per stream. |
| `commit_latency` | histogram | Destination commit duration in seconds. |
| `quorum_waiting_latency` | histogram | Full heartbeat quorum wait in seconds. |
| `request_size_bytes` | counter | Estimated cumulative YQL request size. |
| `replication_lag_estimation` | gauge | Local time minus quorum `step`, seconds. |
| `topic_without_hb{stream_tag}` | gauge | `1` when a heartbeat is late. |
| `go_heap_allocated` | gauge | Current heap size in bytes. |

`max_expected_heartbeat_interval` should exceed the changefeed
`RESOLVED_TIMESTAMPS` interval. Exceeding it emits a warning and sets
`topic_without_hb`, but does not stop replication by itself.

## Build and run

Use the Go version from [`go.mod`](go.mod) and provide network access to both YDB
databases.

```bash
go build -o aardappel ./cmd/aardappel
./aardappel -config ./cmd/aardappel/config.yaml
```

Run checks with:

```bash
go test ./...
```

The process handles `SIGINT`, `SIGTERM`, and `SIGHUP` by cancelling its working
context and releasing the lock.

## Limitations and operational notes

- Replication is one-way: source YDB → destination YDB.
- Destination tables and consumers must exist before startup; only `state_table`
  is created automatically.
- To preserve multi-table consistency, list all CDC streams in one Aardappel
  instance.
- One consumer of one CDC topic must not be read concurrently by independent
  Aardappel instances; YDB would split partitions between them. Use one consumer
  per instance, except for active/standby protected by one lock and `instance_id`.
- Progress is limited by the slowest partition: no new quorum can form without its
  heartbeat.
- Reordering unchanged `src_topic → dst_table` pairs is safe after restart.
  Changing stream membership, mappings, or consumers requires a common-checkpoint
  migration plan.
- Configuration and destination schemas are loaded only at startup; restart
  Aardappel for changes to take effect.
- Run Aardappel in the same network or location as destination YDB. Topic reading
  naturally tolerates source-side delay, while destination transactions use a
  5-second timeout and are not designed for long or unstable cross-region latency.
- For `FATAL_ERROR`, inspect `last_msg` and DLQ first. Changing state to `OK`
  without fixing the cause doesn't resolve the problem.

---

# Aardappel (Русский)

[English](#aardappel) · [Русский](#aardappel-русский)

Aardappel — сервис асинхронной CDC-репликации между базами YDB. Он читает
changefeed-топики исходной базы, восстанавливает общий порядок изменений по
virtual timestamp, а затем атомарно применяет согласованный набор изменений к
таблицам целевой базы.

Главное свойство: изменения из всех настроенных потоков, достигшие общей
heartbeat-границы, записываются в целевые таблицы вместе с новой позицией
репликации в одной YDB-транзакции. Offset сообщений подтверждается только после
успешного commit этой транзакции.

## Содержание

- [Архитектура](#архитектура)
- [Алгоритм репликации](#алгоритм-репликации)
- [Запуск и восстановление](#запуск-и-восстановление)
- [Подготовка YDB](#подготовка-ydb)
- [Конфигурация](#конфигурация)
- [Добавление нового stream](#добавление-нового-stream)
- [Авторизация](#авторизация)
- [Обработка проблемных сообщений](#обработка-проблемных-сообщений)
- [Фильтрация ключей](#фильтрация-ключей)
- [Мониторинг](#мониторинг)
- [Сборка и запуск](#сборка-и-запуск)
- [Ограничения и эксплуатационные замечания](#ограничения-и-эксплуатационные-замечания)

## Архитектура

```mermaid
flowchart LR
    subgraph SRC[Source YDB]
        T1[Source table 1] --> CDC1[CDC 1]
        T2[Source table 2] --> CDC2[CDC 2]

        subgraph TOPIC1[CDC topic 1]
            C1P0[Partition 0]
            C1P1[Partition 1]
            C1PN[Partition N]
        end

        subgraph TOPIC2[CDC topic 2]
            C2P0[Partition 0]
            C2P1[Partition 1]
        end

        CDC1 --> C1P0
        CDC1 --> C1P1
        CDC1 --> C1PN
        CDC2 --> C2P0
        CDC2 --> C2P1
    end

    subgraph APP[Aardappel]
        subgraph RS[Topic read sessions<br/>одна session на stream]
            R1[Partition reader<br/>CDC 1 / partition 0]
            R2[Partition reader<br/>CDC 1 / partition 1]
            RN[Partition reader<br/>CDC 1 / partition N]
            R3[Partition reader<br/>CDC 2 / partition 0]
            R4[Partition reader<br/>CDC 2 / partition 1]
        end
        P[Parser + ordered TxQueue]
        H[Heartbeat tracker]
        B[Batch/query builder]
        L[Distributed lock]
    end

    subgraph DST[Destination YDB]
        D1[Destination table 1]
        D2[Destination table N]
        S[State/lock table]
        KF[Optional key filter table]
        CQ[Optional command topic]
        DLQ[Optional dead-letter topic]
    end

    C1P0 --> R1
    C1P1 --> R2
    C1PN --> RN
    C2P0 --> R3
    C2P1 --> R4
    R1 & R2 & RN & R3 & R4 -->|update / erase| P
    R1 & R2 & RN & R3 & R4 -->|resolved timestamp| H
    P --> B
    H -->|quorum boundary| B
    CQ -. skip / apply .-> RS
    KF -. blocked keys .-> B
    B --> L
    L -->|one YDB transaction| D1
    L -->|one YDB transaction| D2
    L -->|position + stage| S
    RS -. problem details .-> DLQ
```

Один CDC changefeed представлен YDB-топиком и может состоять из нескольких
партиций. Конфигурация `streams` создаёт одну topic read session на CDC-топик, а
YDB SDK назначает внутри неё отдельное чтение каждой партиции. Поэтому на схеме
каждой партиции соответствует свой **partition reader**. Для алгоритма это
отдельный elementary stream с идентификатором `(stream_id, partition_id)`.

Основные компоненты:

- `reader` запускает topic read session для каждого элемента `streams`; внутри
  session сообщения каждой назначенной партиции обрабатываются как отдельный
  поток, разбираются из CDC JSON и передаются в `Processor`;
- `TxQueue` объединяет изменения всех таблиц и сортирует их по `(step, tx_id)`;
- `HeartBeatTracker` хранит последний resolved timestamp каждой партиции каждого
  потока;
- `Processor` выбирает согласованный batch, строит YQL для всех destination-таблиц
  и управляет позицией репликации;
- `DstTable` читает схему целевой таблицы и преобразует CDC-значения в её YDB-типы;
- `Locker` не позволяет двум процессам с одним `instance_id` одновременно писать
  реплику.

## Алгоритм репликации

### Позиция и heartbeat quorum

CDC должен содержать virtual timestamps. Позиция сообщения — лексикографически
упорядоченная пара:

```text
(step_a, tx_id_a) < (step_b, tx_id_b), если
step_a < step_b или (step_a == step_b и tx_id_a < tx_id_b)
```

Resolved timestamp приходит отдельно от partition reader каждой партиции. Один
CDC может содержать несколько партиций, и каждая из них независимо участвует в
расчёте общей границы. Quorum считается готовым, когда heartbeat получен от
**всех партиций всех настроенных CDC-потоков**.
Безопасная граница равна минимальному из этих heartbeat:

```text
quorum = min(last_resolved[stream, partition])
```

Следовательно, все изменения с позицией строго меньше quorum уже могли прийти из
каждой партиции и могут быть применены в глобальном порядке. Изменение с позицией,
равной quorum, попадёт в один из следующих batches.

```mermaid
sequenceDiagram
    participant YT as Source topics
    participant R as Readers
    participant Q as TxQueue
    participant H as HeartbeatTracker
    participant P as Processor
    participant YD as Destination YDB

    YT->>R: update/erase + ts(step, tx_id)
    R->>Q: enqueue change
    YT->>R: resolved timestamp per partition
    R->>H: enqueue heartbeat
    P->>H: wait until every partition has heartbeat
    H-->>P: min heartbeat = quorum
    P->>Q: pop changes with ts < quorum
    P->>YD: UPSERT/DELETE + state position (single transaction)
    YD-->>P: commit OK
    P->>R: commit change offsets
    P->>R: commit quorum heartbeat offset
```

### Формирование запроса

Для одного quorum изменения распределяются по `TableId`, то есть по порядку
элементов в `streams`. Внутри каждой таблицы:

- несколько updates одного ключа объединяются, сохраняя итоговый набор колонок;
- последний тип операции для ключа определяет итоговый `UPSERT` или `DELETE`;
- UPSERT-группы с одинаковым набором колонок объединяются в `AS_TABLE`;
- типы параметров берутся из фактической схемы destination-таблицы.

YQL всех destination-таблиц и UPSERT позиции в `state_table` выполняются одной
транзакцией. Так сохраняется атомарность между таблицами и checkpoint.

### Подтверждение сообщений

Порядок действий принципиален:

1. Выполнить изменения и обновить checkpoint в destination YDB.
2. Получить успешный commit YDB-транзакции.
3. Подтвердить offsets CDC-сообщений.
4. Подтвердить offset quorum heartbeat.

Если процесс завершится между пунктами 2 и 3, сообщения могут быть прочитаны
повторно. При перезапуске они будут отброшены по сохранённой позиции. Поэтому
запись в destination идемпотентна на уровне checkpoint и итоговых UPSERT/DELETE,
но доставка из топика фактически работает как at-least-once.

## Запуск и восстановление

### State table

Таблица создаётся автоматически в destination YDB:

```sql
CREATE TABLE IF NOT EXISTS <state_table> (
    id Utf8,
    step_id Uint64,
    tx_id Uint64,
    state Utf8,
    stage Utf8,
    last_msg Utf8,
    lock_owner Utf8,
    lock_deadline Timestamp,
    PRIMARY KEY (id)
);
```

Одна строка соответствует одному `instance_id` и одновременно хранит:

- checkpoint: `step_id`, `tx_id`;
- состояние: `OK` или `FATAL_ERROR`;
- стадию: `INITIAL_SCAN` или `RUN`;
- последнюю фатальную ошибку;
- владельца и deadline распределённой блокировки.

При первом запуске создаётся строка с позицией `(0, 0)`, состоянием `OK` и стадией
`INITIAL_SCAN`.

### INITIAL_SCAN

Начальная стадия нужна, чтобы перейти к устойчивой общей границе всех потоков:

1. Aardappel ждёт первый полный набор heartbeat.
2. Запоминает максимальный heartbeat в этом наборе.
3. Ждёт следующий quorum, который строго больше этого максимума.
4. Применяет накопленные до этой позиции изменения batches до 1000 сообщений.
5. В последней транзакции сохраняет найденную позицию и переключает stage на
   `RUN`.

### RUN

В обычном режиме каждая итерация ждёт quorum, выбирает все изменения строго до
него, атомарно применяет их и сохраняет quorum как новый checkpoint.

### Перезапуск

При старте `Processor` загружает строку своего `instance_id`. Сообщения и
heartbeat с позицией меньше сохранённой сразу подтверждаются и не применяются.
Если в state table сохранён статус, отличный от `OK`, автоматическое продолжение
не выполняется: сначала требуется устранить причину и осознанно восстановить
состояние.

### Несколько экземпляров

Перед чтением source-топиков процесс получает lock в строке своего `instance_id`.
TTL lock равен `2 × max_expected_heartbeat_interval`.

- `multiple_instances_mode: true` — standby-экземпляры продолжают ждать lock и
  могут подхватить работу после его освобождения;
- `multiple_instances_mode: false` — процесс прекращает ожидание после серии
  неудачных проверок (проверка выполняется раз в 5 секунд).

У процессов, реплицирующих независимые наборы данных через одну state table,
должны быть разные `instance_id`. Для active/standby одной репликации, наоборот,
используется одинаковый `instance_id` и одинаковая конфигурация потоков.

## Подготовка YDB

### Source

Для каждой исходной таблицы нужен changefeed формата JSON в режиме `UPDATES` с
virtual и resolved timestamps, а также consumer:

```bash
YDB_EXEC="ydb -e <DATABASE_ENDPOINT> -d <DATABASE_NAME>"

${YDB_EXEC} 'ALTER TABLE `<TABLE_NAME>` ADD CHANGEFEED `<CDC_NAME>` WITH (
  FORMAT = "JSON",
  MODE = "UPDATES",
  VIRTUAL_TIMESTAMPS = TRUE,
  RESOLVED_TIMESTAMPS = Interval("PT1S"),
  INITIAL_SCAN = TRUE
);'

${YDB_EXEC} 'ALTER TOPIC `<TABLE_NAME>/<CDC_NAME>` ADD CONSUMER `<CONSUMER_NAME>`;'
```

`INITIAL_SCAN = TRUE` нужно включать, если source-таблица уже содержит данные:
тогда changefeed сначала экспортирует существующие строки, а затем продолжает
передавать новые изменения. Для заведомо пустой таблицы первоначальное
сканирование можно не включать. Во время initial scan resolved timestamps могут
временно не поступать, поэтому Aardappel начнёт формировать heartbeat quorum после
завершения сканирования.

Каждая запись `streams` должна ссылаться на такой топик и consumer. Подробнее об
опции первоначального сканирования — в
[документации YDB](https://ydb.tech/docs/ru/yql/reference/syntax/alter_table/changefeed).

### Destination

Destination-таблицы создаются заранее. Для каждой пары source/destination:

- порядок и типы колонок первичного ключа должны быть совместимы;
- CDC-колонки должны существовать в destination-таблице и иметь совместимые типы;
- сервисная учётная запись должна иметь права на чтение схемы и запись данных;
- `state_table` не должна конфликтовать с пользовательской таблицей.

Aardappel не требует полного совпадения схем source и destination. Практическое
требование проще: каждое сообщение из source CDC должно быть возможно применить к
соответствующей destination-таблице. В частности, все переданные в сообщении
колонки должны существовать в destination и преобразовываться в её типы, а ключ
должен соответствовать destination primary key.

Изменения схем выполняются до появления зависящих от них CDC-сообщений. Например,
при добавлении колонки сначала добавьте совместимую колонку в destination, затем в
source и только после этого начинайте записывать в source значения новой колонки.
Иначе Aardappel может получить сообщение с ещё неизвестной destination-схеме
колонкой и остановить репликацию.

Схема каждой destination-таблицы читается один раз при запуске и затем хранится в
памяти. После изменения destination-схемы Aardappel необходимо перезапустить,
прежде чем в source начнут появляться сообщения, использующие новые колонки или
типы. При таком перезапуске сервис повторно выполнит `DescribeTable` и обновит
локальное представление схемы.

## Конфигурация

Файл передаётся флагом `-config`; по умолчанию используется `config.yaml` из
текущего каталога. Полный пример находится в
[`cmd/aardappel/config.yaml`](cmd/aardappel/config.yaml).

Конфигурация читается один раз при старте. Автоматического reload файла нет:
любое изменение connection strings, авторизации, streams, очередей, мониторинга
или других параметров требует перезапуска приложения.

```yaml
src_connection_string: "grpcs://source.example.net:2135/Root/source"
src_client_balancer: true
src_oauth2_file: "/run/secrets/source-oauth.json"
# src_static_token: "..."               # альтернатива src_oauth2_file
# src_oauth2_endpoint: "https://sts.example.net/oauth2/token/exchange"

dst_connection_string: "grpcs://destination.example.net:2135/Root/destination"
dst_client_balancer: true
dst_oauth2_file: "/run/secrets/destination-oauth.json"
# dst_static_token: "..."               # альтернатива dst_oauth2_file
# dst_oauth2_endpoint: "https://sts.example.net/oauth2/token/exchange"

state_table: "/Root/aardappel_state"
instance_id: "orders-replica"
multiple_instances_mode: true
max_expected_heartbeat_interval: 10
log_level: "info"

streams:
  - src_topic: "/Root/orders/orders_cdc"
    consumer: "aardappel"
    dst_table: "/Root/orders_replica"
    problem_strategy: "stop"
    mon_tag: "orders"

mon_server:
  listen: ":8080"

# Опционально:
# cmd_queue:
#   path: "/Root/aardappel_commands"
#   consumer: "aardappel"
# dead_letter_queue:
#   path: "/Root/aardappel_dlq"
# key_filter:
#   table_path: "/Root/aardappel_blocked_keys"
```

### Параметры верхнего уровня

| Параметр | Обязателен | Назначение |
|---|---:|---|
| `src_connection_string` | да | YDB connection string исходной базы. |
| `src_client_balancer` | нет | `false` включает `SingleConn`; полезно, когда прямой доступ к узлам невозможен. |
| `src_oauth2_file` / `src_static_token` | да | Ровно один способ авторизации source. |
| `src_oauth2_endpoint` | нет | Переопределяет endpoint обмена токенов из credentials-файла. |
| `dst_connection_string` | да | YDB connection string целевой базы. |
| `dst_client_balancer` | нет | Аналогичный параметр для destination. |
| `dst_oauth2_file` / `dst_static_token` | да | Ровно один способ авторизации destination. |
| `dst_oauth2_endpoint` | нет | Переопределяет endpoint обмена токенов destination. |
| `state_table` | да | Путь сервисной таблицы состояния и lock в destination. |
| `instance_id` | да | Идентификатор репликации и ключ строки в state table. |
| `multiple_instances_mode` | нет | Разрешает standby-процессу продолжать ожидание lock. |
| `streams` | да | Список соответствий CDC-топиков и destination-таблиц. |
| `max_expected_heartbeat_interval` | рекомендуется | Порог предупреждения об отсутствующем heartbeat; также определяет TTL lock. |
| `log_level` | нет | `debug`, `info`, `warn` или `error`. |
| `mon_server` | нет | Включает HTTP endpoints метрик и readiness. |
| `cmd_queue` | нет | Топик внешних решений `skip`/`apply` для out-of-order сообщений. |
| `dead_letter_queue` | нет | Топик для диагностических сообщений о проблемных транзакциях. |
| `key_filter` | нет | Таблица ключей, изменения которых не нужно применять. |

### Параметры `streams[]`

| Параметр | Обязателен | Назначение |
|---|---:|---|
| `src_topic` | да | Путь к source CDC-топику. |
| `consumer` | да | Consumer в этом топике. |
| `dst_table` | да | Путь к заранее созданной destination-таблице. |
| `problem_strategy` | нет | `stop` (по умолчанию) или `continue`. |
| `mon_tag` | нет | Значение label `stream_tag`; по умолчанию используется `src_topic`. |

Consumer должен быть эксклюзивен для одного одновременно работающего контура
Aardappel. Одинаковое имя consumer допустимо в разных CDC-топиках, но один и тот
же consumer одного топика нельзя совместно читать независимыми экземплярами.

Порядок `streams` не сохраняется в state table: там хранится только общая позиция
`(step_id, tx_id)`. После рестарта конфигурация перечитывается, и `TableId` заново
назначается по текущему индексу. Поэтому существующие элементы можно переставлять,
если каждая пара `src_topic → dst_table` остаётся неизменной.

Добавление, удаление или переназначение stream требует отдельной проверки
checkpoint. Все streams делят одну глобальную позицию: новый поток с историей до
уже сохранённого checkpoint может быть воспринят как старый, и его сообщения будут
пропущены. Для такой миграции может потребоваться новый `instance_id`, новый
consumer/state или предварительное выравнивание данных.

## Добавление нового stream

Новый stream можно добавить в существующий экземпляр репликации, например когда
появилась ещё одна source-таблица, которую нужно согласованно реплицировать вместе
с уже настроенными таблицами. Новый stream войдёт в тот же heartbeat quorum, а его
изменения будут применяться в одной destination-транзакции с изменениями остальных
streams.

Главная задача при добавлении stream — **выровнять его позицию с уже работающей
репликацией**. При этом существуют два независимых уровня позиции:

- `step_id` и `tx_id` в `state_table` образуют один глобальный virtual timestamp
  всех streams экземпляра. По нему Aardappel фильтрует сообщения, которые считает
  уже обработанными;
- каждый CDC consumer отдельно хранит committed offsets всех партиций топика. Они
  определяют, с какого физического сообщения продолжится чтение каждого stream.

Сброс глобального timestamp в `state_table` не откатывает offsets consumers.
Поэтому существующие streams продолжат чтение со своих committed offsets и получат
только новые сообщения, а новый consumer начнёт со своей позиции — обычно с начала
CDC. Через некоторое время streams достигнут общего heartbeat quorum и продолжат
работу в обычном консистентном режиме.

Процедура зависит от состояния и размера нового stream:

```mermaid
flowchart TD
    A[Добавить новый stream] --> B{Source-таблица пустая?}
    B -->|да| C[Добавить stream в конфиг<br/>и перезапустить Aardappel]
    B -->|нет| D{История помещается<br/>в одну транзакцию?}
    D -->|да| E[Остановить репликацию<br/>добавить stream<br/>сбросить timestamp в 0]
    D -->|нет| F[Остановить репликацию<br/>добавить stream<br/>сбросить timestamp в 0<br/>включить INITIAL_SCAN]
    E --> G[Запустить и дождаться<br/>общего quorum]
    F --> H[Aardappel сам применяет initial scan batches<br/>и автоматически переходит в RUN]
```

### Сценарий 1: новая пустая таблица и лёгкий stream

Если source-таблица новая и пустая, а в stream нет истории для применения:

1. Создайте совместимую destination-таблицу, CDC и consumer.
2. Добавьте пару `src_topic → dst_table` в `streams`.
3. Перезапустите Aardappel, чтобы он перечитал конфигурацию и схему destination.
4. Проверьте, что новый stream начал присылать heartbeat и вошёл в общий quorum.

Менять `state_table` в этом сценарии не требуется. CDC и consumer должны быть
созданы до начала записи данных в новую source-таблицу.

### Сценарий 2: непустой stream, помещающийся в одну транзакцию

Этот вариант подходит, если новая таблица и CDC уже содержат данные, но вся история
до первого общего quorum достаточно мала для одной destination-транзакции:

1. Остановите активный процесс и все standby-процессы с тем же `instance_id`.
2. Сохраните текущую строку state и committed offsets всех партиций consumers.
3. Добавьте новый stream в конфигурацию.
4. Сбросьте глобальный timestamp экземпляра в `(0, 0)`, чтобы Aardappel не
   отфильтровал исторические сообщения нового stream по старому checkpoint:

   ```sql
   UPDATE `<state_table>`
   SET step_id = 0, tx_id = 0
   WHERE id = "<instance_id>";
   ```

5. Запустите Aardappel.

Существующие consumers сохранят свои committed offsets, поэтому старые streams
будут читать только сообщения, появившиеся после последних commit. Новый consumer
прочитает свой stream с начала и применит его историю. В некоторый момент все
streams выровняются на общем heartbeat quorum, после чего репликация продолжится в
обычном режиме.

### Сценарий 3: большой stream с initial scan

Этот вариант нужен, если новый CDC содержит initial scan, который не помещается в
один destination-запрос или транзакцию:

1. Остановите активный процесс и все standby-процессы с тем же `instance_id`.
2. Сохраните текущую строку state и committed offsets всех партиций consumers.
3. Добавьте новый stream в конфигурацию.
4. Сбросьте глобальный timestamp в `(0, 0)` и переключите экземпляр в
   `INITIAL_SCAN`:

   ```sql
   UPDATE `<state_table>`
   SET step_id = 0,
       tx_id = 0,
       stage = "INITIAL_SCAN"
   WHERE id = "<instance_id>";
   ```

5. Запустите Aardappel.

В `INITIAL_SCAN` Processor применяет изменения batches не более 1000 CDC-сообщений.
Достигнув синхронизированного heartbeat quorum, он сохранит новую глобальную
позицию, автоматически переключит `stage` в `RUN` и продолжит обычную репликацию.
Существующие streams продолжат чтение со своих committed consumer offsets, а новый
stream прочитает и применит initial-scan историю и догонит их.

### Альтернатива: предварительная репликация отдельным экземпляром

Большую новую таблицу можно сначала реплицировать временным отдельным экземпляром
Aardappel:

1. Создайте для нового stream отдельные `instance_id`, строку state и consumer.
2. Реплицируйте новую таблицу до тех пор, пока временный экземпляр не догонит
   source.
3. Остановите основной и временный экземпляры репликации.
4. Добавьте новый stream в основную конфигурацию и выполните выравнивание из
   сценария 2: сбросьте глобальный timestamp основного экземпляра в `(0, 0)` и
   запустите его.
5. Переиспользуйте либо выровняйте consumer offset по подтверждённой точке
   переключения, чтобы основной экземпляр продолжил с данных, уже применённых
   временной репликацией.
6. После проверки общего quorum, lag и destination-данных удалите временную
   репликацию.

Нельзя допускать, чтобы временный и основной экземпляры одновременно читали один
consumer.

### Переходный период и безопасность

Во время догоняющей репликации или batch-применения initial scan новая
destination-таблица может быть заполнена только частично. Поэтому некоторое время
весь набор destination-таблиц может быть логически неконсистентным, хотя каждый
отдельный batch применяется атомарно. Потребители не должны использовать новую
таблицу до завершения выравнивания либо должны явно поддерживать переходное
состояние.

Ручное изменение `state_table` и consumer offsets связано с риском потери или
повторного применения данных. Выполняйте его только при остановленных readers.
Сохраните исходные state и offsets, убедитесь, что `state = "OK"`, а завершение
выравнивания подтвердите по heartbeat/quorum, replication lag, логам и прикладной
сверке данных.

> **Нельзя одновременно использовать одного consumer одного CDC-топика в разных
> независимо работающих экземплярах Aardappel.** YDB распределит партиции между
> read sessions: один процесс получит одну часть партиций, другой — оставшуюся.
> Каждый `HeartBeatTracker` при этом ожидает heartbeat от всех партиций, поэтому
> полный quorum не сформируется корректно. Для независимых экземпляров создавайте
> разные consumers. Active/standby с одним `instance_id` могут использовать общий
> consumer, поскольку distributed lock допускает к чтению только один процесс.

## Авторизация

Source и destination настраиваются независимо. Для каждой стороны необходимо
задать **ровно один** вариант:

- `*_static_token` — готовый YDB access token без OAuth2 exchange;
- `*_oauth2_file` — JSON-файл параметров OAuth2 token exchange.

Поддерживаются два формата credentials-файла:

1. Нативный формат OAuth2 YDB Go SDK (`token-endpoint`,
   `subject-credentials` и поддерживаемые SDK источники токена).
2. Формат `oauth2_token_exchange`, в котором actor/subject token можно читать из
   отдельного файла или задать значением. Это удобно для projected service account
   token в Kubernetes.

Параметры `src_oauth2_endpoint` и `dst_oauth2_endpoint` имеют приоритет над
endpoint внутри соответствующего credentials-файла.

Полное описание форматов, полей и примеры JSON находятся в
[`internal/auth/README.md`](internal/auth/README.md).

## Обработка проблемных сообщений

Reader отслеживает последний heartbeat каждой партиции. Если после него приходит
изменение с меньшей позицией, поток нарушил ожидаемый порядок.

```mermaid
flowchart TD
    O[Получено out-of-order изменение] --> C{Настроен cmd_queue?}
    C -->|да| F{Найдена команда для<br/>instance + topic + key + ts?}
    F -->|apply| A[Применить изменение]
    F -->|skip| S[Пропустить и commit offset]
    F -->|нет| P{problem_strategy}
    C -->|нет| P
    P -->|continue| D[Записать описание в DLQ,<br/>если она настроена]
    D --> S
    P -->|stop| E[Сохранить FATAL_ERROR]
    E --> SD[Записать проблему в DLQ,<br/>если она настроена]
    SD --> R[Дочитать до следующего heartbeat;<br/>записать остальные out-of-order<br/>транзакции в DLQ]
    R --> X[Остановить процесс]
```

Команда в `cmd_queue` имеет вид:

```json
{
  "aardapel_instance_id": "orders-replica",
  "path": "/Root/orders/orders_cdc",
  "key": ["order-42"],
  "ts": [123456789, 17],
  "action": "skip"
}
```

Допустимые actions: `skip` и `apply`. Поле `aardapel_instance_id` намеренно
приведено с тем же написанием, которое ожидает текущий JSON parser.

`dead_letter_queue` получает текстовое описание ошибки и помогает разбирать
пропущенные или остановившие репликацию события. При стратегии `stop` Aardappel
записывает в DLQ исходную проблему, затем дочитывает stream до следующего heartbeat
и записывает найденные на этом интервале out-of-order транзакции. Запись выполняется
только при настроенной DLQ. Проблемное сообщение в этой ветке не подтверждается;
после сбора диагностики процесс останавливается. DLQ не является источником
автоматического replay.

## Фильтрация ключей

`key_filter.table_path` подключает таблицу заблокированных ключей. При запуске
ключи для текущего `instance_id` загружаются в память; совпавшие изменения не
попадают в YQL destination-транзакции.

Таблица фильтра создаётся оператором заранее и должна поддерживать чтение диапазона
по `instance_id` и запись полей:

```sql
CREATE TABLE `/Root/aardappel_blocked_keys` (
    instance_id Utf8,
    key String,
    PRIMARY KEY (instance_id, key)
);
```

`key` — бинарная сериализация primary key вместе с путём destination-таблицы;
вручную формировать её без использования того же serializer не рекомендуется.

## Мониторинг

Если настроен `mon_server.listen`, сервис публикует:

- `GET /metrics` — Prometheus metrics;
- `GET /readyz` — `200` после успешной проверки source-топиков и destination-таблиц,
  до этого `503`.

При нескольких экземплярах на одном адресе каждому нужен отдельный listen port.

Основные прикладные метрики:

| Метрика | Тип | Значение |
|---|---|---|
| `modifications_count` | counter | Число применённых модификаций. |
| `modifications_count_per_table{stream_tag}` | counter | Модификации по stream. |
| `commit_latency` | histogram | Время commit destination-транзакции, секунды. |
| `quorum_waiting_latency` | histogram | Ожидание полного heartbeat quorum, секунды. |
| `request_size_bytes` | counter | Оценочный суммарный объём YQL-запросов. |
| `replication_lag_estimation` | gauge | Разница локального времени и `step` quorum, секунды. |
| `topic_without_hb{stream_tag}` | gauge | `1`, если heartbeat не пришёл в ожидаемый интервал. |
| `go_heap_allocated` | gauge | Текущий объём heap, байты. |

`max_expected_heartbeat_interval` должен быть больше интервала
`RESOLVED_TIMESTAMPS` changefeed. Его превышение создаёт warning и выставляет
`topic_without_hb`, но само по себе не останавливает репликацию.

## Сборка и запуск

Требуется Go версии из [`go.mod`](go.mod) и сетевой доступ к обеим YDB.

```bash
go build -o aardappel ./cmd/aardappel
./aardappel -config ./cmd/aardappel/config.yaml
```

Проверка проекта:

```bash
go test ./...
```

Процесс корректно обрабатывает `SIGINT`, `SIGTERM` и `SIGHUP`, отменяя рабочий
context и освобождая lock.

## Ограничения и эксплуатационные замечания

- Репликация однонаправленная: source YDB → destination YDB.
- Destination-таблицы и consumer создаются до запуска; автоматически создаётся
  только `state_table`.
- Для согласованности нескольких таблиц все их CDC streams должны быть перечислены
  в одном экземпляре Aardappel.
- Один consumer одного CDC-топика нельзя одновременно читать из разных независимо
  работающих экземпляров Aardappel: YDB разделит партиции между ними. Используйте
  отдельный consumer на экземпляр; исключение — защищённая общим lock пара
  active/standby с одинаковым `instance_id`.
- Прогресс ограничен самой медленной партицией: без heartbeat от неё новый quorum
  сформировать нельзя.
- Перестановка неизменных пар `src_topic → dst_table` допустима после рестарта.
  Изменение состава streams, соответствий или consumer требует плана миграции
  общего checkpoint.
- Конфигурация и схемы destination-таблиц загружаются только при старте. Их
  изменения вступают в силу после перезапуска Aardappel.
- Рекомендуется запускать Aardappel в той же сети или локации, что и destination
  YDB. Topic-reading естественным образом допускает задержки со стороны source, а
  транзакционные запросы к destination выполняются с timeout 5 секунд и не
  рассчитаны на длительные или нестабильные межрегиональные сетевые задержки.
- При `FATAL_ERROR` сначала исследуйте `last_msg` и DLQ; простая смена state на
  `OK` без решения причины не решает проблему.
