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

async def update_job(conn: asyncpg.Connection, status: str, document_id: str, result: str = None , error: str = None):
    await conn.execute(
        "UPDATE jobs SET status = $1, result = $2, error = $3, updated_at = NOW()  WHERE document_id = $4",
        status, result, error, document_id
    )

async def update_chunks(conn: asyncpg.Connection, status: str, job_id: str, result: str = None , error: str = None):
    await conn.execute(
        "UPDATE chunks SET status = $1, result = $2, updated_at = NOW(), error = $4  WHERE id = $3",
        status, result, job_id, error
    )

async def update_chunk(conn: asyncpg.Connection, status: str, chunk_id: str, error: str = None):
    await conn.execute(
        "UPDATE chunks SET status = $1, updated_at = NOW(), error = $2  WHERE id = $3",
        status, error, chunk_id
    )

async def update_all_chunks(conn: asyncpg.Connection, status: str, document_id):
    await conn.execute(
        "UPDATE chunks SET status = $1, updated_at = NOW()  WHERE document_id = $2",
        status, document_id
    )

async def get_total_chunks(conn: asyncpg.Connection, document_id: str):
    return await conn.fetchval(
        """
        Select num_chunks from jobs where document_id = $1
        """
    , document_id)

async def get_all_chunks(conn: asyncpg.Connection, document_id: str):
    rows = await conn.fetch(
        "SELECT * FROM chunks WHERE document_id = $1",
        document_id
    )
    return [
        ChunkRecord(
            id=row["id"],
            document_id=row["document_id"],
            address=json.loads(row["address"]) if isinstance(row["address"], str) else row["address"],
            chunk_index=row["chunk_index"],
            source_text=row["source_text"],
            status=row["status"],
            result=row["result"],
        )
        for row in rows
    ]

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