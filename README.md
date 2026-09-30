# ADBC Driver for Apache SedonaDB

This repository contains the standalone version of Apache SedonaDB's
`sedona-adbc` crate. It implements the ADBC driver, database, connection, and
statement interfaces directly on top of a Sedona context.

The SedonaDB dependencies are pinned to the Apache SedonaDB revision from which
this crate was copied:

- Apache SedonaDB: `242e4ffe329c36dead87f163331d3041409e7aac`

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
| `temp_dir` | Directory for temporary spill files | DataFusion default |
| `memory_pool_type` | `fair` or `greedy` | `fair` |
| `unspillable_reserve_ratio` | Number from `0.0` to `1.0` | `0.2` |

Runtime options cannot be changed after database initialization.
