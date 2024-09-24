# Aardappel

The Aardappel is an lightweight CDC receiver for consistent asynchronous replication of data changes between YDB databases. It is designed to simplify integration between different applications which use the YDB database for persistent storage, or the independently maintained/upgraded instances of a same application. 

### How does it work
The Apprdappel pod receives CDC streams from a source YDB database, restores linearizability of transaction using heartbeat info, and applies modifications atomically into the target YDB database.

### How to create compatible CDC
```
YDB_EXEC="ydb -e <DATABASE_ENDPOINT> -d <DATABASE_NAME>"

${YDB_EXEC} 'alter table <TABLE_NAME> add changefeed <CDC_NAME> with (format="JSON", mode="UPDATES", virtual_timestamps=true, resolved_timestamps = Interval("PT1S"));'
${YDB_EXEC} 'alter topic <TABLE_NAME>/<CDC_NAME> add consumer <CDC_CONSUMER_NAME>;'
```

### How to configure aardappel
[Configuration example](https://github.com/ydb-platform/aardappel/blob/main/cmd/aardappel/config.yaml)

### How to build
Just run
```
go build
```
