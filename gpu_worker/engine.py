from vllm import AsyncLLMEngine, AsyncEngineArgs, EngineArgs, SamplingParams

engine: AsyncLLMEngine | None = None

async def init_engine():
    global engine
    engine_args = AsyncEngineArgs(
        model="distilgpt2",
        dtype="float32",
        tokenizer_mode="auto",
        download_dir=None,
        hf_token="",  # or load from env
        gpu_memory_utilization=0.2,
    )
    engine = AsyncLLMEngine.from_engine_args(engine_args)

def get_engine() -> AsyncLLMEngine:
    return engine