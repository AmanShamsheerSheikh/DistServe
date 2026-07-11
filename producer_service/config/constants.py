from pydantic import BaseModel

KAFKA_INFERENCE_TOPIC = 'inference-request'

class KafkaJobObject(BaseModel):
    id: str
    prompt: str
