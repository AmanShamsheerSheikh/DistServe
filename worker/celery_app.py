from celery import Celery
from kombu import Queue

celery_app = Celery(
    "ml_orchestrator",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1"
)

celery_app.conf.task_queues = (
    Queue("small_jobs", routing_key="small.#"),
    Queue("large_jobs", routing_key="large.#"),
)

celery_app.conf.task_default_queue = "small_jobs"
celery_app.conf.task_default_exchange = "tasks"
celery_app.conf.task_default_routing_key = "small.default"

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Kolkata",
    enable_utc=True,
    worker_prefetch_multiplier=1
)

celery_app.autodiscover_tasks(["worker"])