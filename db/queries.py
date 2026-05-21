from psycopg2.extensions import connection
import asyncpg

async def add_job(db: asyncpg.Connection, job_name: str, status: str) -> int:
    query = """
        INSERT INTO jobs (job_name, status) 
        VALUES ($1, $2) 
        RETURNING id;
    """
    return await db.fetchval(query, job_name, status)

def update_job(conn: connection, status: str, job_id: int):
    with conn.cursor() as cursor:
        query = """
            UPDATE jobs SET status = %s, updated_at = NOW() WHERE id = %s
        """
        cursor.execute(query, (status, job_id))


async def get_job_status(db: asyncpg.Connection, job_id: int) -> str:
    query = """
        SELECT status FROM jobs WHERE id = $1;
    """
    return await db.fetchval(query, job_id)