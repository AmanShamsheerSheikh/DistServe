import asyncpg

async def add_job(db: asyncpg.Connection, status: str, gpu_id: str, prompt: str) -> int:
    query = """
        INSERT INTO jobs (status, gpu_id, prompt)
        VALUES ($1, $2, $3)
        RETURNING id
    """
    return await db.fetchval(query, status, gpu_id, prompt)

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