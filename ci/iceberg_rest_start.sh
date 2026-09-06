#!/bin/sh

set -eu

docker rm --force iceberg-rest-qv >/dev/null 2>&1 || true
docker network inspect qv-test >/dev/null 2>&1 || docker network create qv-test

docker run \
  --detach \
  --name iceberg-rest-qv \
  --network qv-test \
  --publish 127.0.0.1:8181:8181 \
  --env "AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE" \
  --env "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  --env "AWS_REGION=eu-central-1" \
  --env "CATALOG_WAREHOUSE=s3://data/iceberg" \
  --env "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO" \
  --env "CATALOG_S3_ENDPOINT=http://minio:9000" \
  --env "CATALOG_S3_PATH__STYLE__ACCESS=true" \
  apache/iceberg-rest-fixture:1.10.1

trap 'docker logs iceberg-rest-qv >&2 || true' 0

curl \
  --fail \
  --silent \
  --show-error \
  --retry 60 \
  --retry-all-errors \
  --retry-delay 1 \
  --retry-max-time 120 \
  --max-time 10 \
  http://localhost:8181/v1/config >/dev/null

curl \
  --fail \
  --silent \
  --show-error \
  --request POST \
  --header "Content-Type: application/json" \
  --data '{"namespace":["db"],"properties":{}}' \
  http://localhost:8181/v1/namespaces >/dev/null

# Keep this metadata version aligned with the direct-metadata E2E test.
curl \
  --fail \
  --silent \
  --show-error \
  --request POST \
  --header "Content-Type: application/json" \
  --data '{"name":"COVID-19_NYT","metadata-location":"s3://data/iceberg/db/COVID-19_NYT/metadata/v3.metadata.json"}' \
  http://localhost:8181/v1/namespaces/db/register >/dev/null

trap - 0
