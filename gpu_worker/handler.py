import runpod
from engine import init_engine
# from db import init_redis
from runpod.serverless.utils.rp_validator import validate
from discom.constants import runpod_schema
from generator import token_generator
import asyncio

async def handler(job):
    job_input = job["input"]
    print("job: ", job_input)

    validated_input = validate(job_input, runpod_schema)

    if "errors" in validated_input:
        return {"error": validated_input["errors"]}

    generated_text = await token_generator(job_input["prompt"], job_input["job_id"])
    return {
        "generated": generated_text
    }

async def initializations():
    # await init_redis()
    await init_engine()

if __name__ == "__main__":
    asyncio.run(initializations())
    runpod.serverless.start({"handler": handler})