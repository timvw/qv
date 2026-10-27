#!/bin/sh

set -eu

# MinIO no longer publishes images (quay.io and Docker Hub tags are gone), so use
# Chainguard's build. Recent MinIO no longer serves plain files from the data
# directory, so the test data is mirrored into the "data" bucket after startup.
MINIO_IMAGE="cgr.dev/chainguard/minio@sha256:a1d2b3516a41296510f7fb49184c18b88287fa8f35cf3ce5c047289053b94964"
MC_IMAGE="cgr.dev/chainguard/minio-client@sha256:778b2dac231584d5d5749ab663f9f61979d60dd5c00abf682af888db6a9f46e4"
MINIO_ROOT_USER="AKIAIOSFODNN7EXAMPLE"
MINIO_ROOT_PASSWORD="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

docker rm --force minio-qv >/dev/null 2>&1 || true
docker network inspect qv-test >/dev/null 2>&1 || docker network create qv-test

docker run \
  --detach \
  --name minio-qv \
  --network qv-test \
  --network-alias minio \
  --publish 127.0.0.1:9000:9000 \
  --publish 127.0.0.1:9001:9001 \
  --env "MINIO_ROOT_USER=$MINIO_ROOT_USER" \
  --env "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD" \
  "$MINIO_IMAGE" server /data \
  --console-address ":9001"

trap 'docker logs minio-qv >&2 || true' 0

curl \
  --fail \
  --silent \
  --show-error \
  --retry 30 \
  --retry-all-errors \
  --retry-delay 1 \
  --retry-max-time 60 \
  --max-time 5 \
  http://localhost:9000/minio/health/live >/dev/null

mc() {
  docker run \
    --rm \
    --network qv-test \
    --volume "$PWD/testing/data:/src:ro" \
    --env "MC_HOST_minio=http://$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD@minio:9000" \
    "$MC_IMAGE" "$@"
}

mc mb --ignore-existing minio/data >/dev/null
mc mirror --quiet /src minio/data >/dev/null

trap - 0
