#!/bin/sh
# Creates the medallion buckets. Runs once via the minio-init compose service.
set -e

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

for bucket in bronze silver gold checkpoints; do
    mc mb --ignore-existing "local/$bucket"
done

echo "Buckets ready:"
mc ls local
