import asyncio
import logging
from vllm import SamplingParams
import engine as engine_module
from settings import llm_settings
from db import get_redis
import traceback
from discom.logger import setup_logger

logger = setup_logger(log_level=logging.DEBUG)
logger = logging.LoggerAdapter(logger, {"request_id": "N/A"})

async def token_generator(prompt, job_id):
    # client = get_redis()
    stream_key = f"job:{job_id}:tokens"
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
            # new_token = current_text[len(previous_text):]
            previous_text = current_text
            # if new_token:
                # await client.xadd(stream_key, {"data": new_token})
        # await client.xadd(stream_key, {"data": "[DONE]"})
        # await client.set(f"job:{job_id}:result", previous_text, ex=21600)
        # await client.set(f"job:{job_id}:status", "DONE", ex=21600)  
    except asyncio.CancelledError:
        await engine.abort(job_id)
        # await client.xadd(stream_key, {"data": "[CANCELLED]"})
        # await client.set(f"job:{job_id}:status", "CANCELLED", ex=21600)
        raise
    except Exception as e:
        await engine.abort(job_id)
        # await client.xadd(stream_key, {"data": f"[ERROR] {str(e)}"})
        # await client.set(f"job:{job_id}:status", "FAILED")
        # await client.set(f"job:{job_id}:error", tb_str)
    return previous_text