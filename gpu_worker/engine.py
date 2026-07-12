from transformers import AutoConfig, AutoTokenizer
from vllm import AsyncLLMEngine, AsyncEngineArgs
from typing import Any
from settings import llm_settings, api_settings

engine: AsyncLLMEngine | None = None
tokenizer: Any | None = None
context_length: int = 0

async def init_engine():
    print("engine initialized")
    global engine, tokenizer, context_length
    engine_args = AsyncEngineArgs(
        model=llm_settings.model_name,
        hf_token=api_settings.hf_token,
        gpu_memory_utilization=llm_settings.gpu_memory_utilization,
    )
    engine = AsyncLLMEngine.from_engine_args(engine_args)
    tokenizer = AutoTokenizer.from_pretrained(llm_settings.model_name)
    _model_config = AutoConfig.from_pretrained(llm_settings.model_name)
    context_length = _model_config.max_position_embeddings


def get_engine() -> AsyncLLMEngine:
    return engine

def get_tokenizer():
    return tokenizer

def get_context_length():
    return context_length