from aiokafka import AIOKafkaConsumer
from asyncpg import Pool
import asyncpg
from settings import kafka_settings, pg_settings


consumer: AIOKafkaConsumer | None = None
db_pool: Pool | None = None

async def init_consumer():
    global consumer, db_pool
    consumer = AIOKafkaConsumer(
        kafka_settings.KAFKA_TOPIC,
        group_id=kafka_settings.KAFKA_GROUP_ID,
        bootstrap_servers=kafka_settings.KAFKA_BOOTSTRAP_SERVER,
        enable_auto_commit=False
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
    await consumer.start()

def get_consumer():
    return consumer

def get_db_pool():
    return db_pool