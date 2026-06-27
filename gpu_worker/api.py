from contextlib import asynccontextmanager
from fastapi import FastAPI
from vllm import SamplingParams
import engine as engine_module
from schemas import generateRequest

@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine_module.init_engine()
    yield


app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"Hello": "Aman Sheikh"}

@app.post("/generate")
async def generate(request: generateRequest):
    engine = engine_module.get_engine()
    o = ""
    sampling_params = SamplingParams(max_tokens=100)
    async for output in engine.generate(request.prompt, sampling_params=sampling_params, request_id="test"):
        o = output
    return {
        "output": o
    }