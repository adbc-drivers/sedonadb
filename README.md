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

## Parameter binding

Use `Statement::bind()` or `bind_stream()` to supply Arrow parameter values.
Each row executes the query once; streams may contain multiple batches. Numeric
placeholders (`$1`, `$2`, ...) bind by column position, and named placeholders
(`$name`) bind to Arrow fields with matching names (without `$`). Values retain their Arrow types and extension metadata; SQL expression coercion
is left to DataFusion.

`get_parameter_schema()` reports inferred types, using Arrow Null for unknown
types. The first parameter row determines the output schema, including extension
metadata. Schema inspection reads ahead to that row and retains it for execution.
Later rows must produce the same output schema; nullable output fields allow
parameter values to alternate between null and non-null.
`execute()` consumes the binding and returns results lazily; bind again before
another parameterized execution. Bulk ingestion and `execute_update()` remain
unsupported.
