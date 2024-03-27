# Usage

## Run query on data

```bash
qv s3://tpc-h-parquet/1/customer -q 'select c_custkey, UPPER(c_name) from tbl'
```

## View schema of data

```bash
qv ./datasets/tpc-h-parquet/1/customer -s
```

## View data on GCS.

```bash
qv gs://datafusion-delta-testing/data/delta/COVID-19_NYT
```

## View data on S3

### Configuration

Usually [Credential](https://github.com/awslabs/aws-sdk-rust/blob/main/sdk/aws-config/src/default_provider/credentials.rs#L25) loading works out of the box when using the [AWS SDK for Rust](https://github.com/awslabs/aws-sdk-rust/tree/main).  

The following environment variables are needed for credentials:

* AWS_REGION
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY

In case you have AWS SSO credentials you need to set the following:
* AWS_PROFILE

In case you have a custom endpoint in place (eg: [minio](https://min.io/)) you also need to set:
#* AWS_ENDPOINT_URL
AWS_ENDPOINT
AWS_ALLOW_HTTP
https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html



```bash
qv s3://tpc-h-parquet/1/customer
```

## Specify AWS (SSO) profile to use

```bash
qv s3://tpc-h-parquet/1/customer --profile my-user
```

This is the same as:

```bash
AWS_PROFILE=my-user qv s3://tpc-h-parquet/1/customer
```

## View data from S3 console URL

```bash
qv https://s3.console.aws.amazon.com/s3/buckets/datafusion-delta-testing?region=eu-central-1&prefix=simple_table/&showversions=false
``` 

## View data which matches a globbing pattern:

```bash
qv "s3://datafusion-parquet-testing/data/alltypes_pla*n.parquet"
```

## View delta table (no need for a manifest)

```bash
qv /Users/timvw/src/github/delta-rs/rust/tests/data/COVID-19_NYT
```

## View delta table at specific point in time

```bash
qv /Users/timvw/src/github/delta-rs/rust/tests/data/COVID-19_NYT --at "2022-01-01T16:39:00+01:00"
```

## View glue table

```bash
qv glue://mydb.table1
```