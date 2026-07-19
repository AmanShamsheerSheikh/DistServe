FROM python:3.11-slim

WORKDIR /app
COPY distserver-common/ ./common/
RUN pip install --no-cache-dir ./common
COPY producer_service/ ./producer_service/
RUN pip install --no-cache-dir -r producer_service/requirements.txt
WORKDIR /app/producer_service
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]