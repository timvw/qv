# Quickly view your data (qv)

A simply CLI to quickly view your data. Powered by [DataFusion](https://github.com/apache/arrow-datafusion).

## Features
* View file (and directories of files) contents
* Run SQL against files
* View file schemas
* Supports [Deltalake](https://delta.io/) (No need for manifest file), CSV, JSON, [Parquet](https://parquet.apache.org/) and [Avro](https://avro.apache.org/) file formats
* Supports local file system and s3.

## Usage

### View local data

```bash
qv /mnt/datasets/nyc/green_tripdata_2020-07.csv
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

### View data on s3

```bash
qv s3://tpc-h-parquet/1/customer
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | c_address                             | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                                         |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
| 1         | Customer#000000001 | IVhzIApeRb ot,c,E                     | 15          | 25-989-741-2988 | 711.56    | BUILDING     | to the even, regular platelets. regular, ironic epitaphs nag e                                                    |
| 2         | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak        | 13          | 23-768-687-3665 | 121.65    | AUTOMOBILE   | l accounts. blithely ironic theodolites integrate boldly: caref                                                   |
| 3         | Customer#000000003 | MG9kdTD2WBHm                          | 1           | 11-719-748-3364 | 7498.12   | AUTOMOBILE   | deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov             |
| 4         | Customer#000000004 | XxVSJsLAGtn                           | 4           | 14-128-190-5944 | 2866.83   | MACHINERY    | requests. final, regular ideas sleep final accou                                                                  |
| 5         | Customer#000000005 | KvpyuHCplrB84WgAiGV6sYpZq7Tj          | 3           | 13-750-942-6364 | 794.47    | HOUSEHOLD    | n accounts will have to unwind. foxes cajole accor                                                                |
| 6         | Customer#000000006 | sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn  | 20          | 30-114-968-4951 | 7638.57   | AUTOMOBILE   | tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious          |
| 7         | Customer#000000007 | TcGe5gaZNgVePxU5kRrvXBfkasDTea        | 18          | 28-190-982-9759 | 9561.95   | AUTOMOBILE   | ainst the ironic, express theodolites. express, even pinto beans among the exp                                    |
| 8         | Customer#000000008 | I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5 | 17          | 27-147-574-9335 | 6819.74   | BUILDING     | among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide |
| 9         | Customer#000000009 | xKiAFTjUsCuxfeleNqefumTrjS            | 8           | 18-338-906-3675 | 8324.07   | FURNITURE    | r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl                     |
| 10        | Customer#000000010 | 6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2    | 5           | 15-741-346-9870 | 2753.54   | HOUSEHOLD    | es regular deposits haggle. fur                                                                                   |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
```

### View delta table (no need for a manifest)

```bash
qv /Users/timvw/src/github/delta-rs/rust/tests/data/COVID-19_NYT
+------------+------------------+-----------+-------+-------+--------+
| date       | county           | state     | fips  | cases | deaths |
+------------+------------------+-----------+-------+-------+--------+
| 2021-02-25 | Caddo            | Louisiana | 22017 | 24637 | 672    |
| 2021-02-25 | Calcasieu        | Louisiana | 22019 | 19181 | 354    |
| 2021-02-25 | Caldwell         | Louisiana | 22021 | 1076  | 25     |
| 2021-02-25 | Cameron          | Louisiana | 22023 | 543   | 5      |
| 2021-02-25 | Catahoula        | Louisiana | 22025 | 1053  | 34     |
| 2021-02-25 | Claiborne        | Louisiana | 22027 | 1407  | 49     |
| 2021-02-25 | Concordia        | Louisiana | 22029 | 1770  | 55     |
| 2021-02-25 | De Soto          | Louisiana | 22031 | 2635  | 71     |
| 2021-02-25 | East Baton Rouge | Louisiana | 22033 | 35389 | 732    |
| 2021-02-25 | East Carroll     | Louisiana | 22035 | 1094  | 23     |
+------------+------------------+-----------+-------+-------+--------+
```

### Run query on data
```bash
qv s3://tpc-h-parquet/1/customer -q 'select c_custkey, UPPER(c_name) from tbl'
+-----------+--------------------+
| c_custkey | upper(tbl.c_name)  |
+-----------+--------------------+
| 1         | CUSTOMER#000000001 |
| 2         | CUSTOMER#000000002 |
| 3         | CUSTOMER#000000003 |
| 4         | CUSTOMER#000000004 |
| 5         | CUSTOMER#000000005 |
| 6         | CUSTOMER#000000006 |
| 7         | CUSTOMER#000000007 |
| 8         | CUSTOMER#000000008 |
| 9         | CUSTOMER#000000009 |
| 10        | CUSTOMER#000000010 |
+-----------+--------------------+
```

### View schema of data
```bash
qv ./datasets/tpc-h-parquet/1/customer -s
+--------------+-----------+-------------+
| column_name  | data_type | is_nullable |
+--------------+-----------+-------------+
| c_custkey    | Int64     | NO          |
| c_name       | Utf8      | YES         |
| c_address    | Utf8      | YES         |
| c_nationkey  | Int64     | NO          |
| c_phone      | Utf8      | YES         |
| c_acctbal    | Float64   | NO          |
| c_mktsegment | Utf8      | YES         |
| c_comment    | Utf8      | YES         |
+--------------+-----------+-------------+
```

## Installation

### As a homebrew package
```bash
brew tap timvw/tap
brew install qv
```

### Download a binary from github releases
```bash
wget https://github.com/timvw/qv/releases/download/v0.1.5/qv_v0.1.5_x86_64-apple-darwin.tar.gz
tar -zxf qv_v0.1.5_x86_64-apple-darwin.tar.gz
```

### As a container image
```bash
docker run --rm -it -v $HOME/.aws:/root/.aws -e AWS_PROFILE=icteam docker.io/timvw/qv:latest s3://datafusion-testing/data/avro/alltypes_plain.avro
```

### Via rust toolchain
```bash
cargo install qv
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