from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
import asyncpg
import redis.asyncio as redis
from db.db_connection import get_db_connection, get_redis
from db.queries import add_job
from worker.tasks import run_ml_training_job
from pydantic import BaseModel
from contants import settings
from api.calls import fetch_runpod_gpu_catalog

class TrainingRequest(BaseModel):
    job_name: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up: Initializing database pools...")
    
    app.state.db_pool = await asyncpg.create_pool(
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        host=settings.POSTGRES_HOST,
        port=settings.PGBOUNCER_PORT,
        database=settings.POSTGRES_DB,
        min_size=5,
        max_size=20,
        statement_cache_size=0
    )
    
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
    
@app.get("/get_gpu_list")
async def get_gpus():
    gpu_catalog = fetch_runpod_gpu_catalog()
    if "error" in gpu_catalog:
        raise HTTPException(status_code=500, detail=gpu_catalog["error"])
    return gpu_catalog