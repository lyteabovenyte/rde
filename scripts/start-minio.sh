#!/bin/bash

# Start MinIO container for local development
echo "Starting MinIO container..."

docker run -d -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  --name minio-server \
  quay.io/minio/minio server /data --console-address ":9001"

echo "MinIO started!"
echo "Access MinIO Console at: http://localhost:9001"
echo "Access MinIO API at: http://localhost:9000"
echo "Username: minioadmin"
echo "Password: minioadmin"
echo ""
echo "To create a bucket for Iceberg tables:"
echo "1. Go to http://localhost:9001"
echo "2. Login with minioadmin/minioadmin"
echo "3. Create a bucket named 'iceberg-bucket'"
