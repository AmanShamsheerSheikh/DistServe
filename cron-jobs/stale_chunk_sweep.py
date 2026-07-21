import asyncio
import asyncpg
from discom.constants import JobStatus
from settings import pg_settings, redis_settings, kafka_settings
import redis.asyncio as redis
import json
from aiokafka import AIOKafkaProducer
import os

STALE_RUNNING_THRESHOLD_SECONDS = 300
STALE_PENDING_THRESHOLD_SECONDS = 600

async def main():
    pool = await asyncpg.create_pool(
        user=pg_settings.POSTGRES_USER, password=pg_settings.POSTGRES_PASSWORD,
        host=pg_settings.POSTGRES_HOST, port=pg_settings.PGBOUNCER_PORT,
        database=pg_settings.POSTGRES_DB, statement_cache_size=0,
    )
    redis_client = redis.Redis(
        host=redis_settings.REDIS_HOST, port=redis_settings.REDIS_PORT,
        db=redis_settings.REDIS_DB, password=redis_settings.REDIS_PASSWORD,
        decode_responses=redis_settings.REDIS_DECODE_RESPONSES,
    )
    producer = AIOKafkaProducer(bootstrap_servers=kafka_settings.KAFKA_BOOTSTRAP_SERVER, enable_idempotence=True)
    await producer.start()

    lua_path = os.path.join(os.path.dirname(__file__), "chunks_counter.lua")
    with open(lua_path) as f:
        lua_script = f.read()

    async with pool.acquire() as connection:
        stale_chunks = await connection.fetch(
            """
            SELECT id, document_id FROM chunks
            WHERE (status = $1 AND updated_at < NOW() - INTERVAL '1 second' * $2)
               OR (status = $3 AND updated_at < NOW() - INTERVAL '1 second' * $4)
            """,
            JobStatus.RUNNING.value, STALE_RUNNING_THRESHOLD_SECONDS,
            JobStatus.PENDING.value, STALE_PENDING_THRESHOLD_SECONDS,
        )

    for row in stale_chunks:
        chunk_id, document_id = row["id"], row["document_id"]
        print(f"Sweeping stale chunk {chunk_id} for document {document_id}")
        async with pool.acquire() as connection:
            await connection.execute(
                "UPDATE chunks SET status = $1, error = $2, updated_at = NOW() WHERE id = $3",
                JobStatus.FAILED.value, "Swept: stale/orphaned chunk", chunk_id
            )
            total_chunks = await connection.fetchval(
                "SELECT num_chunks FROM jobs WHERE document_id = $1", document_id
            )
        execute_lua = redis_client.register_script(lua_script)
        is_all_chunks_done = await execute_lua(keys=[document_id], args=[total_chunks])
        if is_all_chunks_done:
            payload = json.dumps({"document_id": document_id}).encode("utf-8")
            await producer.send_and_wait(kafka_settings.KAFKA_JOIN_TOPIC, payload)
            await redis_client.delete(document_id)

    await producer.stop()
    await pool.close()

if __name__ == '__main__':
    asyncio.run(main())