docker run \
  --detach \
  --name minio-qv \
  --rm \
  --publish 9000:9000 \
  --publish 9001:9001 \
  --volume "$PWD/testing:/data" \
  --env "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
  --env "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  quay.io/minio/minio:RELEASE.2022-05-26T05-48-41Z server /data \
  --console-address ":9001"