from pydantic import BaseModel

class GenerateRequest(BaseModel):
    prompt: str

class RegisterRequest(BaseModel):
    user_name: str