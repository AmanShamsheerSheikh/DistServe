import asyncio
from vllm import SamplingParams
import gpu_worker.engine as engine_module
from config.settings import llm_settings
from db.queries import update_job
from config.enums import JobStatus

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