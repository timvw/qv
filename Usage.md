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

For an S3-compatible service, set `AWS_ENDPOINT_URL_S3` (or the global
`AWS_ENDPOINT_URL`) to its endpoint. Set
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
AWS_ENDPOINT_URL_S3="https://gateway.storjshare.io" \
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

## View an Iceberg table

Pass either the table root or a specific Iceberg metadata file. qv reads the
Iceberg snapshot and manifest metadata, rather than treating the data directory
as a collection of unrelated Parquet files.

```bash
qv s3://my-warehouse/analytics/events
qv /data/warehouse/analytics/events/metadata/v3.metadata.json
```

Iceberg time travel uses the newest snapshot at or before the supplied RFC 3339
timestamp:

```bash
qv s3://my-warehouse/analytics/events --at 2026-01-01T00:00:00Z
```

## View glue table

For an Iceberg table, qv loads the table through the Glue catalog. Other Glue
tables keep using their storage descriptor and SerDe settings.

```bash
qv glue://mydb.table1
```

## View a table through an Iceberg REST catalog

With `--rest-catalog`, the positional argument is a namespace-qualified table
identifier instead of a filesystem path.

```bash
qv analytics.events \
  --rest-catalog http://localhost:8181 \
  --catalog-warehouse s3://my-warehouse
```

Non-sensitive catalog and storage settings can be repeated as Iceberg
`KEY=VALUE` properties. Source secrets from environment variables so they do
not appear in the process argument list or shell history:

```bash
qv analytics.events \
  --rest-catalog https://catalog.example.com \
  --catalog-property-env token=ICEBERG_TOKEN \
  --catalog-property header.X-Tenant=analytics
```

These property options can also configure direct Iceberg paths and Glue-backed
Iceberg tables. Use `AWS_ENDPOINT_URL_GLUE` when overriding the Glue service
endpoint; the S3 endpoint remains independently configurable.
