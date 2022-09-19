# Quickly view your data (qv)

A simply CLI to quickly view your data. Powered by [DataFusion](https://github.com/apache/arrow-datafusion).

## Features
* View file (and directories of files) contents
* Run SQL against files
* View file schemas
* Supports [Deltalake](https://delta.io/) (No need for manifest file), CSV, JSON, [Parquet](https://parquet.apache.org/) and [Avro](https://avro.apache.org/) file formats
* Supports local file system and S3 (and https links from AWS S3 console).

## Usage

### View data on local filesystem
```
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

### View data on S3
```
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

### View data from S3 console URL
```
qv https://s3.console.aws.amazon.com/s3/buckets/datafusion-delta-testing?region=eu-central-1&prefix=simple_table/&showversions=false
+----+
| id |
+----+
| 9  |
| 5  |
| 7  |
+----+
```

### View data which matches a globbing pattern:
```
qv "s3://datafusion-parquet-testing/data/alltypes_pla*n.parquet"
+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+
| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |
+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+
| 4  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30332f30312f3039 | 30         | 2009-03-01 00:00:00 |
| 5  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30332f30312f3039 | 31         | 2009-03-01 00:01:00 |
| 6  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30342f30312f3039 | 30         | 2009-04-01 00:00:00 |
| 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01 00:01:00 |
| 2  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30322f30312f3039 | 30         | 2009-02-01 00:00:00 |
| 3  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30322f30312f3039 | 31         | 2009-02-01 00:01:00 |
| 0  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30312f30312f3039 | 30         | 2009-01-01 00:00:00 |
| 1  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30312f30312f3039 | 31         | 2009-01-01 00:01:00 |
+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+
```

### Run query on data
```
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
```
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

### View delta table (no need for a manifest)

~~The current implementation depends (partially) on [Rusoto](https://github.com/rusoto/rusoto) which does not work well with AWS profiles.
As a workaround you can export (temporary) tokens and use those as following:~~

The deltalake implementation has been reworked and (re-)uses the credentials which are acquired by [aws_config](https://docs.rs/aws-config/latest/aws_config/).

```
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


## Installation

### As a [Homebrew](https://brew.sh/) package
```bash
brew tap timvw/tap
brew install qv
```

### Download a binary from [Github Release](https://github.com/timvw/qv/releases/latest)
```bash
wget https://github.com/timvw/qv/releases/download/v0.1.23/qv-0.1.23-x86_64-apple-darwin-generic.tar.gz
tar -zxf qv-0.1.23-x86_64-apple-darwin-generic.tar.gz
```

### Run as a [container](https://github.com/timvw/qv/pkgs/container/qv) image
```bash
docker run --rm -it -v $HOME/.aws:/root/.aws -e AWS_PROFILE=icteam ghcr.io/timvw/qv:0.1.23 s3://datafusion-testing/data/avro/alltypes_plain.avro
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