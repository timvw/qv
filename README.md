# Quickly view your data (qv)

A simply CLI to quickly view your data. Powered by [DataFusion](https://github.com/apache/arrow-datafusion).

## Features

* View file (and directories of files) contents
* Run SQL against files
* View file schemas
* Supported formats:
  - [Deltalake](https://delta.io/) (No need for manifest file)
  - [Iceberg](https://iceberg.apache.org/)
  - [Parquet](https://parquet.apache.org/)
  - [Avro](https://avro.apache.org/)
  - CSV, 
  - JSON,
* Supported storage sytems: 
  - local file system
  - [S3](https://aws.amazon.com/s3/) (+ https links from AWS S3 console)
  - [GCS](https://cloud.google.com/storage)

## Usage

### View data on local filesystem

```bash
qv /mnt/datasets/nyc/green_tripdata_2020-07.csv
```

Example output:

```
+----------+----------------------+-----------------------+--------------------+------------+--------------+--------------+-----------------+---------------+-------------+-------+---------+------------+--------------+-----------+-----------------------+--------------+--------------+-----------+----------------------+
| VendorID | lpep_pickup_datetime | lpep_dropoff_datetime | store_and_fwd_flag | RatecodeID | PULocationID | DOLocationID | passenger_count | trip_distance | fare_amount | extra | mta_tax | tip_amount | tolls_amount | ehail_fee | improvement_surcharge | total_amount | payment_type | trip_type | congestion_surcharge |
+----------+----------------------+-----------------------+--------------------+------------+--------------+--------------+-----------------+---------------+-------------+-------+---------+------------+--------------+-----------+-----------------------+--------------+--------------+-----------+----------------------+
| 2        | 2020-07-01 00:05:18  | 2020-07-01 00:22:07   | N                  | 1          | 134          | 35           | 2               | 6.38          | 20.5        | 0.5   | 0.5     | 0          | 0            |           | 0.3                   | 21.8         | 2            | 1         | 0                    |
| 2        | 2020-07-01 00:47:06  | 2020-07-01 00:52:13   | N                  | 1          | 41           | 42           | 1               | 1.06          | 6           | 0.5   | 0.5     | 1.46       | 0            |           | 0.3                   | 8.76         | 1            | 1         | 0                    |
| 2        | 2020-07-01 00:24:59  | 2020-07-01 00:35:18   | N                  | 1          | 42           | 159          | 1               | 2.1           | 9           | 0.5   | 0.5     | 0          | 0            |           | 0.3                   | 10.3         | 2            | 1         | 0                    |
| 2        | 2020-07-01 00:55:12  | 2020-07-01 00:58:45   | N                  | 1          | 116          | 116          | 1               | 0.7           | 5           | 0.5   | 0.5     | 0          | 0            |           | 0.3                   | 6.3          | 2            | 1         | 0                    |
| 2        | 2020-07-01 00:12:36  | 2020-07-01 00:20:14   | N                  | 1          | 43           | 141          | 1               | 1.84          | 8           | 0.5   | 0.5     | 0          | 0            |           | 0.3                   | 12.05        | 2            | 1         | 2.75                 |
| 2        | 2020-07-01 00:30:55  | 2020-07-01 00:37:05   | N                  | 5          | 74           | 262          | 1               | 2.04          | 27          | 0     | 0       | 0          | 0            |           | 0.3                   | 30.05        | 2            | 1         | 2.75                 |
| 2        | 2020-07-01 00:13:00  | 2020-07-01 00:19:09   | N                  | 1          | 159          | 119          | 1               | 1.35          | 6.5         | 0.5   | 0.5     | 0          | 0            |           | 0.3                   | 7.8          | 2            | 1         | 0                    |
| 2        | 2020-07-01 00:39:09  | 2020-07-01 00:40:55   | N                  | 1          | 75           | 75           | 1               | 0.35          | -3.5        | -0.5  | -0.5    | 0          | 0            |           | -0.3                  | -4.8         | 4            | 1         | 0                    |
| 2        | 2020-07-01 00:39:09  | 2020-07-01 00:40:55   | N                  | 1          | 75           | 75           | 1               | 0.35          | 3.5         | 0.5   | 0.5     | 0          | 0            |           | 0.3                   | 4.8          | 2            | 1         | 0                    |
| 2        | 2020-07-01 00:45:59  | 2020-07-01 01:01:02   | N                  | 1          | 75           | 87           | 1               | 8.17          | 24          | 0.5   | 0.5     | 4.21       | 0            |           | 0.3                   | 32.26        | 1            | 1         | 2.75                 |
+----------+----------------------+-----------------------+--------------------+------------+--------------+--------------+-----------------+---------------+-------------+-------+---------+------------+--------------+-----------+-----------------------+--------------+--------------+-----------+----------------------+
```

### View data on GCS.

```bash
qv gs://datafusion-delta-testing/data/delta/COVID-19_NYT
```

### View data on S3

```bash
qv s3://tpc-h-parquet/1/customer
```

### Specify AWS (SSO) profile to use

```bash
qv s3://tpc-h-parquet/1/customer --profile my-user
```

### View data from S3 console URL

```bash
qv https://s3.console.aws.amazon.com/s3/buckets/datafusion-delta-testing?region=eu-central-1&prefix=simple_table/&showversions=false
``` 

### View data which matches a globbing pattern:

```bash
qv "s3://datafusion-parquet-testing/data/alltypes_pla*n.parquet"
```

### View delta table (no need for a manifest)

```bash
qv /Users/timvw/src/github/delta-rs/rust/tests/data/COVID-19_NYT
```

### View delta table at specific point in time

```bash
qv /Users/timvw/src/github/delta-rs/rust/tests/data/COVID-19_NYT --at "2022-01-01T16:39:00+01:00"
```

### View glue table

```bash
qv glue://mydb.table1
```

### Run query on data

```bash
qv s3://tpc-h-parquet/1/customer -q 'select c_custkey, UPPER(c_name) from tbl'
```

### View schema of data

```bash
qv ./datasets/tpc-h-parquet/1/customer -s
```

## Installation

### As a [Homebrew](https://brew.sh/) package

```bash
brew tap timvw/tap
brew install qv
```

### Download a binary from [Github Release](https://github.com/timvw/qv/releases/latest)
```bash
wget https://github.com/timvw/qv/releases/download/v0.4.0/qv-0.4.0-x86_64-apple-darwin-generic.tar.gz
tar -zxf qv-0.4.0-x86_64-apple-darwin-generic.tar.gz
```

### Run as a [container](https://github.com/timvw/qv/pkgs/container/qv) image

```bash
docker run --rm -it -v $HOME/.aws:/root/.aws -e AWS_PROFILE=icteam ghcr.io/timvw/qv:0.4.0 s3://datafusion-testing/data/avro/alltypes_plain.avro
```

### Via rust toolchain

```bash
cargo install --git https://github.com/timvw/qv --tag v0.4.0
```

## Development

Uses standard rust toolchain:

```bash
cargo build
cargo test
cargo publish 
```

Linting:

```bash
cargo fmt
cargo clippy --all-features --all-targets --workspace -- -D warnings
cargo tomlfmt -p ./Cargo.toml
```

Or all in one as following:

```bash
./dev/rust_lint.sh
```
