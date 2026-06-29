import asyncpg
from fastapi import FastAPI
from config.settings import pg_settings
import redis.asyncio as redis


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
        host='localhost',
        port=6379,
        db=0,
        password=None,
        decode_responses=True
    )
    app.state.redis = redis.Redis(connection_pool=redis_pool)