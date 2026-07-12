import asyncpg
import json
from discom.constants import ChunkRecord

async def get_user_id(db: asyncpg.Connection, api_key: str):
    query = """
        SELECT user_id from users where api_key = $1
    """
    return await db.fetchval(query, api_key)

async def add_job(db: asyncpg.Connection, user_id: str, document_id: str, file_name: str, num_chunks: int, status: str) -> int:
    query = """
        INSERT INTO jobs (user_id, document_id, file_name, num_chunks, status)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
    """
    return await db.fetchval(query, user_id, document_id, file_name, num_chunks, status)

async def register_user(conn: asyncpg.Connection, user_name: str):
    query = """
        INSERT INTO users (user_name)
        VALUES ($1)
        RETURNING api_key
    """
    return await conn.fetchval(query, user_name)

async def get_user(conn: asyncpg.Connection, api_key: str):
    query = """
        SELECT user_name from users where api_key = $1
    """
    return await conn.fetchval(query, api_key)

async def update_job(conn: asyncpg.Connection, status: str, job_id: str, result: str = None , error: str = None):
    await conn.execute(
        "UPDATE jobs SET status = $1, result = $2, error = $3, updated_at = NOW()  WHERE id = $4",
        status, result, error, job_id
    )

async def update_chunks(conn: asyncpg.Connection, status: str, job_id: str, result: str = None , error: str = None):
    await conn.execute(
        "UPDATE chunks SET status = $1, result = $2, updated_at = NOW()  WHERE id = $3",
        status, result, job_id
    )

async def bulk_insert_chunks(db: asyncpg.Connection, chunks: list[ChunkRecord]):
    await db.executemany(
        """
        INSERT INTO chunks (id, document_id, chunk_index, address, source_text, status)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        [
            (chunk.id, chunk.document_id, chunk.chunk_index, json.dumps(chunk.address), chunk.source_text, chunk.status)
            for chunk in chunks
        ],
    )