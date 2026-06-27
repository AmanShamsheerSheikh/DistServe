from pydantic import BaseModel

class generateRequest(BaseModel):
    prompt: str