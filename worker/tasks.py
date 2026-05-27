from worker.celery_app import celery_app
from db.queries import update_job
from psycopg2 import pool
from contants import settings

db_pool = pool.ThreadedConnectionPool(
    minconn=2,
    maxconn=10,
    user=settings.POSTGRES_USER,
    password=settings.POSTGRES_PASSWORD,
    host=settings.POSTGRES_HOST,
    port=settings.PGBOUNCER_PORT,
    database=settings.POSTGRES_DB,
)

@celery_app.task
def run_ml_training_job(job_id: int, job_name: str):
    conn = db_pool.getconn()
    try:
        update_job(conn, 'STARTED', job_id)
        conn.commit()
        print(f"Executing ML job: {job_name}...")
        import time
        time.sleep(10)
        update_job(conn, 'COMPLETED', job_id)
        conn.commit()
    except Exception as e:
        print(f"{job_name} failed with error: {e}")
        conn.rollback()
        update_job(conn, 'FAILED', job_id)
        conn.commit()
    finally:
        db_pool.putconn(conn)