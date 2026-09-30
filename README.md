# ADBC Driver for Apache SedonaDB

This repository provides an [ADBC](https://arrow.apache.org/adbc/) driver for
[Apache SedonaDB](https://github.com/apache/sedona-db). It reuses the
comprehensive [DataFusion ADBC driver](https://github.com/adbc-drivers/datafusion)
and initializes every database with a Sedona-enabled session context.

The upstream Git dependencies are pinned for reproducible builds. Both pins use
DataFusion 54.1:

- Apache SedonaDB: `84d9b7bea8e9449e81358dd5669b8942c2c75779`
- DataFusion ADBC driver: `818eaaf791f68e9224dc6cd91f7ed0cfc08f4102`

## Development

```sh
cargo build --locked
cargo test --locked
cargo clippy --locked --no-deps -- -D warnings
```

The default `ffi` feature exports `AdbcSedonadbDriverInit` from the generated
shared library.
