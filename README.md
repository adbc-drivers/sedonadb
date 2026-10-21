# ADBC Driver for Apache SedonaDB

This repository contains the standalone version of Apache SedonaDB's
`sedona-adbc` crate. It implements the ADBC driver, database, connection, and
statement interfaces directly on top of a Sedona context.


## Development

```sh
cargo build --locked
cargo test --locked
cargo clippy --locked --no-deps -- -D warnings
```

The crate exports `AdbcSedonadbDriverInit` through `adbc_ffi`.

## Database options

The following options are applied when the database is initialized and shared
by all connections created from it:

| Option | Value | Default |
| --- | --- | --- |
| `memory_limit` | Bytes or a size such as `4gb`; `unlimited` disables the limit | `unlimited` |
| `temp_directory` | Directory for temporary spill files | DataFusion default |
| `memory_pool_type` | `fair` or `greedy` | `fair` |
| `unspillable_reserve_ratio` | Number from `0.0` to `1.0` | `0.2` |

Runtime options cannot be changed after database initialization.

## Connection options

The driver supports the standard ADBC `adbc.connection.autocommit`,
`adbc.connection.catalog`, and `adbc.connection.db_schema` options. Catalog and
schema values must already exist. They are applied directly to DataFusion's
session configuration without executing SQL.

## Statement options

Statements track the standard ADBC bulk-ingestion options
`adbc.ingest.target_table`, `adbc.ingest.target_catalog`,
`adbc.ingest.target_db_schema`, and `adbc.ingest.mode`. The
`adbc.ingest.temporary` option accepts `false`; temporary ingestion is not yet
supported.

## Cancellation

`Connection::cancel()` cancels outstanding queries and metadata reads on that
connection. `Statement::cancel()` cancels outstanding executions of that statement
without affecting sibling statements. Both methods are repeatable, and the same
connection and statements can execute new work after cancellation.

Each operation captures the current Tokio cancellation tokens from its handles.
Cancellation replaces the handle's token and cancels the previous generation.
Planning and pending batch reads select on those tokens; cancellation drops the
DataFusion future or output stream. Readers check cancellation before returning
prefetched batches, report one cancellation error, and then return end-of-stream.
An idle reader observes cancellation when next read or dropped. Cancellation is
cooperative and does not preempt synchronous work inside a future's poll.

Completed, failed, or dropped operations release their tokens automatically;
handles do not retain a registry of operations. With the Rust ADBC traits,
`cancel()` requires mutable access to the handle, so a connection can cancel a
statement executing on another thread, and a statement can cancel its returned
reader while that reader is being consumed on another thread.
