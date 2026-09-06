#!/bin/sh

docker rm --force minio-qv >/dev/null 2>&1 || true
docker network rm qv-test >/dev/null 2>&1 || true
