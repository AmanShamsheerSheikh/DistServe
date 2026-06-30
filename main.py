import asyncio
from contextlib import asynccontextmanager
import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Header
from fastapi.responses import StreamingResponse
from vllm import SamplingParams
import gpu_worker.engine as engine_module
from gpu_worker.schemas import GenerateRequest, RegisterRequest
from config.settings import llm_settings
from db import init_db
from db.queries import add_job, update_job, register_user, get_user
from db.connections import get_db_connection, get_redis
from config.enums import JobStatus
import redis.asyncio as redis
from middleware.rate_limiter import RateLimitMiddleWare
from middleware.api_auth import AuthMiddleWare

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up: Initializing database pools...")
    await engine_module.init_engine()
    await init_db.initialize_db(app)
    yield
    print("Shutting down: Closing database pools...")
    await app.state.db_pool.close()
    await app.state.redis.aclose()

app = FastAPI(lifespan=lifespan)
app.add_middleware(RateLimitMiddleWare)
app.add_middleware(AuthMiddleWare)

@app.get("/")
async def read_root():
    return {"Hello": "Aman Sheikh"}

async def token_generator(prompt, job_id, db):
    await update_job(db, JobStatus.RUNNING.value, job_id)
    context_length = engine_module.get_context_length()
    tokenizer = engine_module.get_tokenizer()
    max_prompt_tokens = len(tokenizer.encode(prompt))
    max_tokens = min(context_length - max_prompt_tokens, llm_settings.max_tokens)
    sampling_params = SamplingParams(max_tokens=max_tokens, temperature=llm_settings.temperature)
    engine = engine_module.get_engine()
    previous_text = ""
    try:
        async for output in engine.generate(prompt, sampling_params=sampling_params, request_id=job_id):
            current_text = output.outputs[0].text
            new_token = current_text[len(previous_text):]
            previous_text = current_text
            if new_token:
                yield f"data: {new_token}\n\n"
        yield f"data: [DONE]\n\n"
        await update_job(db, JobStatus.DONE.value, job_id, previous_text)
    except asyncio.CancelledError:
        await engine.abort(job_id)
        raise
    except Exception as e:
        yield f"data: [ERROR] {str(e)}\n\n"
        await update_job(db, JobStatus.FAILED.value, job_id, previous_text, str(e))

@app.post("/generate")
async def generate(request: GenerateRequest, db: asyncpg.Connection = Depends(get_db_connection), redis: redis.Redis = Depends(get_redis)):
    id = await add_job(db, JobStatus.PENDING.value, "0", request.prompt)
    return StreamingResponse(
        token_generator(request.prompt, id, db),
        media_type="text/event-stream"
    )

@app.post("/register_user")
async def register(request: RegisterRequest, db: asyncpg.Connection = Depends(get_db_connection)):
    api_key = await register_user(db, request.user_name)
    return {
        "api_key": api_key
    }