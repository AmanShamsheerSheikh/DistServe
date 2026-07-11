from enum import Enum
from pydantic import BaseModel

class JobStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"

class KafkaJobObject(BaseModel):
    id: str
    prompt: str

runpod_schema = {
    "prompt": {
        "type": str,
        "required": True
    },
    "job_id": {
        "type": str,
        "required": True
    }
}

class GenerateRequest(BaseModel):
    prompt: str

class RegisterRequest(BaseModel):
    user_name: str
