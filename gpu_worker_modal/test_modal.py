"""
Minimal Modal app to validate vLLM generation works on Modal's infra.
No Kafka, no Postgres, no Redis — just confirms the model loads and generates.

This mirrors Modal's own verified vLLM example as closely as possible
(same CUDA base tag + vLLM version pairing they use in their gpt-oss guide)
to minimize the chance of hitting an unverified dependency combination.

Deploy + test with:
    modal run test_gpu_worker.py

(modal run executes the @app.local_entrypoint below immediately — good for
one-off testing. Once you're happy with it, `modal deploy` instead to make
it a persistent, callable-by-name function for consumer_service to reach.)
"""

import modal

app = modal.App("distserve-gpu-worker-test")

MODEL_NAME = "Qwen/Qwen2.5-0.5B-Instruct"

image = (
    modal.Image.from_registry("nvidia/cuda:12.9.0-devel-ubuntu22.04", add_python="3.12")
    .entrypoint([])
    .uv_pip_install("vllm==0.21.0", "redis", "asyncpg", "aiokafka", "huggingface_hub[hf_transfer]==0.36.0")
    .add_local_dir("distserver-common", remote_path="/root/distserver-common", copy=True)
    .run_commands("pip install /root/distserver-common")
    .env({"HF_HUB_ENABLE_HF_TRANSFER": "1"})
    .add_local_python_source("db", "engine", "settings", "discom")
    .add_local_file("chunks_counter.lua", remote_path="/root/chunks_counter.lua")
)

hf_cache_vol = modal.Volume.from_name("hf-cache", create_if_missing=True)


@app.cls(
    image=image,
    gpu="A10G",          # swap for whatever tier you want to test
    min_containers=0,    # scale to zero when idle — no cost while unused
    max_containers=1,
    scaledown_window=60, # seconds to stay warm after last request
    volumes={"/root/.cache/huggingface": hf_cache_vol},
    timeout=600,         # generous ceiling for cold start + first-time compile
)
class GPUWorker:
    @modal.enter()
    async def start(self):
        from vllm import AsyncLLMEngine, AsyncEngineArgs
        from transformers import AutoTokenizer

        self.engine = AsyncLLMEngine.from_engine_args(
            AsyncEngineArgs(
                model=MODEL_NAME,
                enforce_eager=False,  # fine on real cloud GPUs; True was only needed for WSL2
                gpu_memory_utilization=0.4,
            )
        )
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    @modal.method()
    async def generate(self, prompt: str) -> str:
        from vllm import SamplingParams

        messages = [{"role": "user", "content": prompt}]
        formatted_prompt = self.tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )

        sampling_params = SamplingParams(max_tokens=256, temperature=0.2)

        previous_text = ""
        async for output in self.engine.generate(
            formatted_prompt, sampling_params=sampling_params, request_id=prompt[:16]
        ):
            previous_text = output.outputs[0].text

        return previous_text


@app.local_entrypoint()
def main():
    worker = GPUWorker()
    result = worker.generate.remote("Translate to Hindi: Hello, how are you?")
    print("RESULT:", result)