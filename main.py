from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
import asyncpg
import redis.asyncio as redis
from db.db_connection import get_db_connection, get_redis
from db.queries import add_job
from worker.tasks import run_ml_training_job
from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

db_user = os.environ.get("POSTGRES_USER")
db_password = os.environ.get("POSTGRES_PASSWORD")
db_name = os.environ.get("POSTGRES_DB")

class TrainingRequest(BaseModel):
    job_name: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up: Initializing database pools...")
    
    app.state.db_pool = await asyncpg.create_pool(
        user=db_user,
        password=db_password,
        host='localhost',
        port=6432,
        database=db_name,
        min_size=5,
        max_size=20,
        statement_cache_size=0
    )

    with open("db/sp/create_table.sql", "r") as f:
        schema_sql = f.read()

    async with app.state.db_pool.acquire() as connection:
        await connection.execute(schema_sql)
    
    redis_pool = redis.ConnectionPool(
        host='localhost',
        port=6379,
        db=0,
        password=None,
        decode_responses=True
    )
    app.state.redis = redis.Redis(connection_pool=redis_pool)

    yield

    print("Shutting down: Closing database pools...")
    await app.state.db_pool.close()
    await app.state.redis.aclose()

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def read_root():
    return {"Hello": "Aman Sheikh"}

@app.get("/add_data")
async def add_data(db: asyncpg.Connection = Depends(get_db_connection)):
    job_id = await add_job(db, 'testing', 'PENDING')
    return {"message": "Success", "job_id": job_id}

@app.get("/check_cache")
async def check_cache(cache: redis.Redis = Depends(get_redis)):
    await cache.set("test_key", "Redis is alive!")
    value = await cache.get("test_key")
    return {"redis_response": value}

@app.post("/start_training")
async def start_training(trainingRequest: TrainingRequest, db: asyncpg.Connection = Depends(get_db_connection)):
    job_name = trainingRequest.job_name
    job_id = await add_job(db, job_name, 'PENDING')
    task = run_ml_training_job.apply_async(
        args=[job_id, job_name], 
        queue="large_jobs"
    )
    return {
        "message": "Training job successfully queued!",
        "job_id": job_id,
        "celery_task_id": task.id
    }
    