# Docker Compose Setup with MinIO and Litestream

This setup runs datas3t with MinIO (a local S3-compatible object storage) and Litestream for database backup and restore.

## Overview

The docker-compose configuration consists of these services:

1. **minio**: Local S3-compatible object storage
2. **createbuckets**: One-time service to create the S3 bucket in MinIO
3. **restore**: A one-time service that runs before datas3t starts to restore the database from MinIO
4. **server**: The main datas3t application
5. **aggregator**: Service that periodically aggregates dataranges
6. **replicate**: A service that continuously backs up the database to MinIO

## Prerequisites

- Docker and Docker Compose installed

## Getting Started

1. Copy the example environment file:
   ```bash
   cp .env.minio.example .env
   ```

2. You can customize the environment variables if needed:
   ```
   MINIO_ROOT_USER=minioadmin
   MINIO_ROOT_PASSWORD=minioadmin
   S3_BUCKET=datas3t-bucket
   AGGREGATION_INTERVAL=5m
   ```

3. Start the services:
   ```bash
   docker-compose -f docker-compose.yml up -d
   ```

## How It Works

- The `minio` service provides S3-compatible object storage
- The `createbuckets` service ensures the required S3 bucket exists in MinIO
- The `restore` service runs initially to check if a database backup exists and restores it if found
- The `server` service runs the main datas3t application with these configurations:
  - Database path: `/app/data/datas3t.db`
  - S3 endpoint: `http://minio:9000`
  - S3 region: `us-east-1`
  - S3 credentials: `minioadmin`
  - S3 bucket: `datas3t-bucket`
  - Uploads path: `/app/data/uploads`
- The `aggregator` service periodically aggregates dataranges every 5 minutes
- The `replicate` service continuously backs up the database to MinIO

## Accessing MinIO Console

You can access the MinIO web console at:
- URL: http://localhost:9001
- Username: minioadmin (or the value of MINIO_ROOT_USER)
- Password: minioadmin (or the value of MINIO_ROOT_PASSWORD)

## Configuration

You can modify the `litestream.yml` file to adjust backup settings.

You can also configure the aggregator service by setting:
- `DATAS3T_AGGREGATION_INTERVAL`: How often the aggregation process runs (default: 5m)

## Troubleshooting

- Check logs for each service:
  ```bash
  docker-compose logs minio
  docker-compose logs createbuckets
  docker-compose logs restore
  docker-compose logs server
  docker-compose logs aggregator
  docker-compose logs replicate
  ```

- Manually trigger a backup:
  ```bash
  docker-compose exec replicate litestream snapshot -config /etc/litestream.yml /data/datas3t.db
  ``` 