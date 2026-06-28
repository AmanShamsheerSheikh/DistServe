from psycopg2.extensions import connection
import asyncpg
# from typing import Optional

async def add_job(db: asyncpg.Connection, status: str, gpu_id: str, prompt: str) -> int:
    query = """
        INSERT INTO jobs (status, gpu_id, prompt)
        VALUES ($1, $2, $3)
        RETURNING id
    """
    return await db.fetchval(query, status, gpu_id, prompt)

async def update_job(conn: connection, status: str, job_id: int, error: str = None):
    if not error:
        await conn.execute(
            "UPDATE jobs SET status = $1, error = $2, updated_at = NOW() WHERE id = $3",
            status, error, job_id
        )
    else:
        await conn.execute(
            "UPDATE jobs SET status = $1, updated_at = NOW(), error = $2 WHERE id = $3",
            status, error, job_id
        )