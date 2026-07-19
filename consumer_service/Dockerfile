FROM python:3.11-slim

WORKDIR /app
COPY distserver-common/ ./common/
RUN pip install --no-cache-dir ./common
COPY consumer_service/ ./consumer_service/
RUN pip install --no-cache-dir -r consumer_service/requirements.txt
WORKDIR /app/consumer_service
CMD ["python", "-u", "consumer.py"]