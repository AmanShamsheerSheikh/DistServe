import asyncpg


async def update_job(conn: asyncpg.Connection, status: str, job_id: str, result: str = None , error: str = None):
    await conn.execute(
        "UPDATE jobs SET status = $1, result = $2, error = $3, updated_at = NOW()  WHERE id = $4",
        status, result, error, job_id
    )