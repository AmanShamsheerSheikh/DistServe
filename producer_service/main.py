from contextlib import asynccontextmanager
import asyncpg
from fastapi import Depends, FastAPI
from discom.constants import GenerateRequest, RegisterRequest
from db.init_db import initialize_db
from discom.queries import add_job, register_user
from db.connections import get_db_connection
from discom.constants import JobStatus
import redis.asyncio as redis
from middleware.rate_limiter import RateLimitMiddleWare
from middleware.api_auth import AuthMiddleWare
from discom.constants import KafkaJobObject
from settings import kafka_settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up: Initializing database pools...")
    await initialize_db(app)
    yield
    print("Shutting down: Closing database pools...")
    await app.state.db_pool.close()
    await app.state.redis.aclose()
    await app.state.kafka_producer.stop()

app = FastAPI(lifespan=lifespan)
app.add_middleware(RateLimitMiddleWare)
app.add_middleware(AuthMiddleWare)

@app.get("/")
async def read_root():
    return {"Hello": "Aman Sheikh"}

@app.post("/generate")
async def generate(request: GenerateRequest, db: asyncpg.Connection = Depends(get_db_connection)):
    id = await add_job(db, JobStatus.PENDING.value, "0", request.prompt)
    producer_object: KafkaJobObject = KafkaJobObject(id=str(id), prompt=request.prompt).model_dump_json().encode("utf-8")
    await app.state.kafka_producer.send_and_wait(kafka_settings.KAFKA_INFERENCE_TOPIC, producer_object)
    return {
        "job_id": id,
        "status": "queued"
    }

@app.post("/register_user")
async def register(request: RegisterRequest, db: asyncpg.Connection = Depends(get_db_connection)):
    api_key = await register_user(db, request.user_name)
    return {
        "api_key": api_key
    }