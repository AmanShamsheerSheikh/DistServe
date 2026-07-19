FROM python:3.11-slim

WORKDIR /app
COPY distserver-common/ ./common/
RUN pip install --no-cache-dir ./common
COPY assembler_service/ ./assembler_service/
RUN pip install --no-cache-dir -r assembler_service/requirements.txt
WORKDIR /app/assembler_service
CMD ["python", "-u", "assembler_service.py"]