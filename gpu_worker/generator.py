import asyncio
import json
import logging
import redis
from vllm import SamplingParams
import engine as engine_module
from settings import llm_settings, kafka_settings
from db import get_redis, get_pg_pool, get_lua_script, get_kafka_producer
import traceback
from discom.logger import setup_logger
from discom.queries import update_chunks, get_total_chunks, get_all_chunk_ids
from discom.constants import JobStatus

logger = setup_logger(log_level=logging.DEBUG)
logger = logging.LoggerAdapter(logger, {"request_id": "N/A"})


async def check_if_done(redis: redis.asyncio.Redis, document_id: str, limit: int, lua_script: str):
    execute_lua = redis.register_script(lua_script)
    result = await execute_lua(keys=[document_id], args=[limit])
    return result

async def generate(engine, formatted_prompt, sampling_params, chunk_id):
    previous_text = ""
    async for output in engine.generate(formatted_prompt, sampling_params=sampling_params, request_id=chunk_id):
        current_text = output.outputs[0].text
        previous_text = current_text
    return previous_text

async def token_generator(source_text, chunk_id, document_id, text_type):
    client = get_redis()
    pool = get_pg_pool()
    producer = get_kafka_producer()
    context_length = engine_module.get_context_length()
    tokenizer = engine_module.get_tokenizer()
    messages = None
    if text_type == "paragraph":
        messages = [
            {
                "role": "system",
                "content": "You are a professional translator. Translate the user's text into Hindi. Preserve the original meaning, tone, and formatting exactly. Respond with ONLY the translated text — no explanations, no notes, no quotation marks."
            },
            {"role": "user", "content": source_text}
        ]
    else:
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a professional translator. You will receive a JSON array of strings. "
                    "Translate each string into Hindi. Return ONLY a valid JSON array of the same "
                    "length, in the same order, with no additional text, explanation, or markdown formatting."
                )
            },
            {"role": "user", "content": source_text}
        ]
    formatted_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    max_prompt_tokens = len(tokenizer.encode(formatted_prompt))
    max_tokens = min(context_length - max_prompt_tokens, llm_settings.max_tokens)
    sampling_params = SamplingParams(max_tokens=max_tokens, temperature=llm_settings.temperature)
    engine = engine_module.get_engine()
    previous_text = ""
    total_chunks = 0
    async with pool.acquire() as connection:
        total_chunks = await get_total_chunks(connection, document_id)
    try:
        previous_text = await asyncio.wait_for(
            generate(engine, formatted_prompt, sampling_params, chunk_id),
            timeout=120
        )
        async with pool.acquire() as connection:
            await update_chunks(connection, JobStatus.DONE.value, chunk_id, previous_text, None)
        lua_script = get_lua_script()
        is_all_chunks_done = await check_if_done(client, document_id, total_chunks, lua_script)
        if is_all_chunks_done:
            payload = json.dumps({"document_id": document_id}).encode("utf-8")
            await producer.send_and_wait(kafka_settings.KAFKA_JOIN_TOPIC, payload)
            await client.delete(document_id)
    except asyncio.TimeoutError:
        await engine.abort(chunk_id)
        async with pool.acquire() as connection:
            await update_chunks(connection, JobStatus.FAILED.value, chunk_id, previous_text, "Generation timed out")
    except asyncio.CancelledError:
        await engine.abort(chunk_id)
        async with pool.acquire() as connection:
            await update_chunks(connection, JobStatus.CANCELLED.value, chunk_id, previous_text, None)
        raise
    except Exception:
        tb_str = traceback.format_exc()
        async with pool.acquire() as connection:
            await update_chunks(connection, JobStatus.FAILED.value, chunk_id, previous_text, tb_str)
        await engine.abort(chunk_id)
    return previous_text