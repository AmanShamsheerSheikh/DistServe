FROM runpod/pytorch:2.2.0-py3.10-cuda12.1.1-devel-ubuntu22.04

WORKDIR /app
COPY distserver-common/ ./common/
RUN pip install --no-cache-dir ./common
COPY gpu_worker/ ./gpu_worker/
RUN pip install --no-cache-dir -r gpu_worker/requirements.txt
WORKDIR /app/gpu_worker
CMD ["python", "-u", "handler.py"]