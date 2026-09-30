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
