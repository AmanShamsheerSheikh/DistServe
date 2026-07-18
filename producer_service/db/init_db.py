import asyncpg
from fastapi import FastAPI
from settings import pg_settings, redis_settings, kafka_settings, s3_settings
import redis.asyncio as redis
from aiokafka import AIOKafkaProducer
import boto3
import botocore


async def initialize_db(app: FastAPI):
    app.state.db_pool = await asyncpg.create_pool(
        user=pg_settings.POSTGRES_USER,
        password=pg_settings.POSTGRES_PASSWORD,
        host=pg_settings.POSTGRES_HOST,
        port=pg_settings.PGBOUNCER_PORT,
        database=pg_settings.POSTGRES_DB,
        min_size=pg_settings.PGBOUNCER_MIN_CONNECTIONS,
        max_size=pg_settings.PGBOUNCER_MAX_CONNECTIONS,
        statement_cache_size=0
    )

    redis_pool = redis.ConnectionPool(
        host=redis_settings.REDIS_HOST,
        port=redis_settings.REDIS_PORT,
        db=redis_settings.REDIS_DB,
        password=redis_settings.REDIS_PASSWORD,
        decode_responses=redis_settings.REDIS_DECODE_RESPONSES
    )
    app.state.redis = redis.Redis(connection_pool=redis_pool)

    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=kafka_settings.KAFKA_BOOTSTRAP_SERVER
    )

    app.s3 = boto3.client(
        "s3",
        endpoint_url=s3_settings.S3_HOST,
        aws_access_key_id=s3_settings.MINIO_ROOT_USER,
        aws_secret_access_key=s3_settings.MINIO_ROOT_PASSWORD,
    )

    try:
        app.s3.head_bucket(Bucket=s3_settings.S3_BUCKET)
    except botocore.exceptions.ClientError:
        app.s3.create_bucket(Bucket=s3_settings.S3_BUCKET)

    await app.state.kafka_producer.start()