FROM runpod/pytorch:2.8.0-py3.11-cuda12.8.1-cudnn-devel-ubuntu22.04

RUN pip install uv && uv pip install vllm --torch-backend=auto --system
WORKDIR /app
COPY distserver-common/ ./common/
RUN pip install --no-cache-dir ./common
COPY gpu_worker/ ./gpu_worker/
RUN pip install --no-cache-dir -r gpu_worker/requirements.txt
WORKDIR /app/gpu_worker
CMD ["python", "-u", "handler.py"]