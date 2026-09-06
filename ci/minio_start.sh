#!/bin/sh

set -eu

docker rm --force minio-qv >/dev/null 2>&1 || true
docker network inspect qv-test >/dev/null 2>&1 || docker network create qv-test

docker run \
  --detach \
  --name minio-qv \
  --network qv-test \
  --network-alias minio \
  --publish 127.0.0.1:9000:9000 \
  --publish 127.0.0.1:9001:9001 \
  --volume "$PWD/testing:/data" \
  --env "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
  --env "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  quay.io/minio/minio:RELEASE.2022-05-26T05-48-41Z server /data \
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

trap - 0
