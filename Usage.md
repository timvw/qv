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

### Configuration

QV expects the environment variable 'GOOGLE_APPLICATION_CREDENTIALS' to exist and point to a file which contains google credentials.

```bash
qv gs://datafusion-delta-testing/data/delta/COVID-19_NYT
```

## View data on S3

### Configuration

Usually [Credential](https://github.com/awslabs/aws-sdk-rust/blob/main/sdk/aws-config/src/default_provider/credentials.rs#L25) loading works out of the box when using the [AWS SDK for Rust](https://github.com/awslabs/aws-sdk-rust/tree/main).  

The following environment variables are needed for static credentials:

* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`

You can optionally set `AWS_DEFAULT_REGION`. When it is not set, qv defaults it
to `eu-central-1`.

In case you have AWS SSO credentials you need to set the following:

* `AWS_PROFILE`

For an S3-compatible service, set `AWS_ENDPOINT_URL` to its endpoint. Set
`AWS_ALLOW_HTTP=true` only when the endpoint uses plain HTTP, such as a local
[MinIO](https://min.io/) development server. See the
[`AmazonS3Builder` configuration](https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html)
for the complete list of supported environment variables.

```bash
qv s3://tpc-h-parquet/1/customer
```

### Storj through its S3-compatible gateway

Create S3 credentials in Storj and store them in a named AWS profile so the
secret does not appear in your shell history:

```ini
# ~/.aws/credentials
[storj]
aws_access_key_id = your-storj-access-key
aws_secret_access_key = your-storj-secret-key
```

Then provide the Storj gateway endpoint and select that profile. Storj is
globally distributed, so qv's default region does not select a storage region.

```bash
AWS_ENDPOINT_URL="https://gateway.storjshare.io" \
qv s3://my-bucket/path/to/data.parquet --profile storj
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
