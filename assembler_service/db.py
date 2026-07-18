from aiokafka import AIOKafkaConsumer
from asyncpg import Pool
import asyncpg
from settings import kafka_settings, pg_settings, s3_settings
import boto3


consumer: AIOKafkaConsumer | None = None
db_pool: Pool | None = None
s3 = None

async def init_db():
    global consumer, db_pool, s3
    consumer = AIOKafkaConsumer(
        kafka_settings.KAFKA_JOIN_TOPIC,
        group_id=kafka_settings.KAFKA_GROUP_ID,
        bootstrap_servers=kafka_settings.KAFKA_BOOTSTRAP_SERVER,
        enable_auto_commit=False,
        auto_offset_reset="earliest"
    )
    db_pool = await asyncpg.create_pool(
        user=pg_settings.POSTGRES_USER,
        password=pg_settings.POSTGRES_PASSWORD,
        host=pg_settings.POSTGRES_HOST,
        port=pg_settings.PGBOUNCER_PORT,
        database=pg_settings.POSTGRES_DB,
        min_size=pg_settings.PGBOUNCER_MIN_CONNECTIONS,
        max_size=pg_settings.PGBOUNCER_MAX_CONNECTIONS,
        statement_cache_size=0
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_settings.S3_HOST,
        aws_access_key_id=s3_settings.MINIO_ROOT_USER,
        aws_secret_access_key=s3_settings.MINIO_ROOT_PASSWORD,
    )
    await consumer.start()

def get_consumer():
    return consumer

def get_db_pool():
    return db_pool

def get_s3():
    return s3