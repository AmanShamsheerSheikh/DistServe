from worker.celery_app import celery_app
from db.queries import update_job
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

db_user = os.environ.get("POSTGRES_USER")
db_password = os.environ.get("POSTGRES_PASSWORD")
db_name = os.environ.get("POSTGRES_DB")

@celery_app.task
def run_ml_training_job(job_id: int, job_name: str):
    conn = psycopg2.connect(
        user=db_user,
        password=db_password,
        host='localhost',
        port=6432,
        database=db_name,
    )
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
        conn.close()