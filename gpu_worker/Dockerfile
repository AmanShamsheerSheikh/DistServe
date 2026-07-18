FROM runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04

RUN pip install uv && uv pip install vllm --torch-backend=cu124 --system
WORKDIR /app
COPY distserver-common/ ./common/
RUN pip install --no-cache-dir ./common
COPY gpu_worker/ ./gpu_worker/
RUN pip install --no-cache-dir -r gpu_worker/requirements.txt
WORKDIR /app/gpu_worker
CMD ["python", "-u", "handler.py"]