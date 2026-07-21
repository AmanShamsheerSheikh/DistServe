from aiokafka import AIOKafkaProducer

from settings import redis_settings, pg_settings, kafka_settings
import redis.asyncio as redis
import asyncpg
import os

redisConnection: redis.Redis | None = None
pg_pool: asyncpg.Pool | None = None
kafka_producer: AIOKafkaProducer | None = None
lua_script = None

async def init_db():
    global redisConnection, pg_pool, lua_script, kafka_producer
    pool = redis.ConnectionPool(
        host=redis_settings.REDIS_HOST,
        port=redis_settings.REDIS_PORT,
        db=redis_settings.REDIS_DB,
        password=redis_settings.REDIS_PASSWORD,
        decode_responses=redis_settings.REDIS_DECODE_RESPONSES
    )
    pg_pool = await asyncpg.create_pool(
        user=pg_settings.POSTGRES_USER,
        password=pg_settings.POSTGRES_PASSWORD,
        host=pg_settings.POSTGRES_HOST,
        port=pg_settings.PGBOUNCER_PORT,
        database=pg_settings.POSTGRES_DB,
        min_size=pg_settings.PGBOUNCER_MIN_CONNECTIONS,
        max_size=pg_settings.PGBOUNCER_MAX_CONNECTIONS,
        statement_cache_size=0
    )
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=kafka_settings.KAFKA_BOOTSTRAP_SERVER,
        enable_idempotence=True,
        retry_backoff_ms=200,
        transaction_timeout_ms=60000,
        request_timeout_ms=40000 
    )
    await kafka_producer.start()
    lua_path = os.path.join(os.path.dirname(__file__), "chunks_counter.lua")
    with open(lua_path, "r") as file:
        lua_script = file.read()
    redisConnection = redis.Redis(connection_pool=pool)

def get_redis():
    return redisConnection

def get_pg_pool():
    return pg_pool

def get_lua_script():
    return lua_script

def get_kafka_producer():
    return kafka_producer