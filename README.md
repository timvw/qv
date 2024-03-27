# Quickly view your data (qv)

[<img alt="github" src="https://img.shields.io/badge/github-timvw/qv-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/timvw/qv)
[<img alt="crates.io" src="https://img.shields.io/crates/v/qv.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/qv)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-qv-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/qv)
[<img alt="build status" src="https://img.shields.io/github/actions/workflow/status/timvw/qv/test_suite.yml?branch=main&style=for-the-badge" height="20">](https://github.com/timvw/qv/actions?query=branch%3Amain)

A simply CLI to quickly view your data. Powered by [DataFusion](https://github.com/apache/arrow-datafusion).

## Example: View data on local filesystem

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

For more examples consult our [Usage examples](Usage.md).

## Features

* View file (and directories of files) contents
* Run SQL against files
* View file schemas
* Supported formats:
  - [Deltalake](https://delta.io/)
  - [Parquet](https://parquet.apache.org/)
  - [Avro](https://avro.apache.org/)
  - [CSV](https://en.wikipedia.org/wiki/Comma-separated_values)
  - [NDJSON](http://ndjson.org/)
* Supported storage sytems: 
  - local file system
  - [S3](https://aws.amazon.com/s3/) (+ https links from AWS S3 console)
  - [GCS](https://cloud.google.com/storage)

## Installation
Read the [Installation instructions](Installation.md).

## Development
Read the [Development instructions](Development.md).


