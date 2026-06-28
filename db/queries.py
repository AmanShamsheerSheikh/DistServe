import asyncpg

async def add_job(db: asyncpg.Connection, status: str, gpu_id: str, prompt: str) -> int:
    query = """
        INSERT INTO jobs (status, gpu_id, prompt)
        VALUES ($1, $2, $3)
        RETURNING id
    """
    return await db.fetchval(query, status, gpu_id, prompt)

async def update_job(conn: asyncpg.Connection, status: str, job_id: str, error: str = None):
    await conn.execute(
        "UPDATE jobs SET status = $1, error = $2, updated_at = NOW() WHERE id = $3",
        status, error, job_id
    )