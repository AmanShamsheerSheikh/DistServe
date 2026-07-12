from enum import Enum
from pydantic import BaseModel
from dataclasses import dataclass
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

@dataclass
class ChunkRecord:
    id: str
    document_id: str
    address: any
    chunk_index: int
    source_text: str
    status: str
    result: str