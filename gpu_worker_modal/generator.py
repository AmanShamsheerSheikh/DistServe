import modal


app = modal.App("distserve-gpu-worker")

image = (
    modal.Image.from_registry("nvidia/cuda:12.9.0-devel-ubuntu22.04", add_python="3.12")
    .entrypoint([])
    .uv_pip_install(
        "vllm==0.21.0",
        requirements=["gpu_worker/requirements.txt"],
    )
    .add_local_dir("distserver-common", remote_path="/root/distserver-common", copy=True)
    .run_commands("pip install /root/distserver-common")
    .env({"HF_HUB_ENABLE_HF_TRANSFER": "1"})
    .add_local_python_source("db", "engine", "settings")
    .add_local_file("chunks_counter.lua", remote_path="/root/chunks_counter.lua")
)

hf_cache_vol = modal.Volume.from_name("hf-cache", create_if_missing=True)

@app.cls(
    image=image,
    gpu="A10G",
    min_containers=0,
    max_containers=1,
    scaledown_window=60,
    volumes={"/root/.cache/huggingface": hf_cache_vol},
    timeout=600,
    secrets=[modal.Secret.from_name("distserve-env")]
)
class GPUWorker:
    @modal.enter()
    async def start(self):
        from discom.logger import setup_logger
        from db import init_db
        from engine import init_engine
        import logging

        await init_engine()
        await init_db()
        logger = setup_logger(log_level=logging.DEBUG)
        self.logger = logging.LoggerAdapter(logger, {"request_id": "N/A"})

    @modal.method()
    async def token_generator(self, source_text, chunk_id, document_id, text_type):
        from db import get_redis, get_pg_pool, get_lua_script, get_kafka_producer
        import engine as engine_module
        import asyncio
        import json
        import redis
        from vllm import SamplingParams
        from settings import llm_settings, kafka_settings
        import traceback
        from discom.queries import update_chunks, get_total_chunks
        from discom.constants import JobStatus
        client = get_redis()
        pool = get_pg_pool()
        producer = get_kafka_producer()
        context_length = engine_module.get_context_length()
        tokenizer = engine_module.get_tokenizer()
        messages = None

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