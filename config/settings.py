from typing import Tuple, Type
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, YamlConfigSettingsSource

class LLMSettings(BaseSettings):
    model_name: str
    temperature: float
    max_tokens: int
    gpu_memory_utilization: float
    model_config = SettingsConfigDict(yaml_file="config/model.yaml", env_file_encoding="utf-8")
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        **kwargs
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (YamlConfigSettingsSource(settings_cls),)

class PGSettings(BaseSettings):
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str
    PGBOUNCER_PORT: int
    PGBOUNCER_POOL_MODE: str
    PGBOUNCER_MAX_CLIENT_CONN: int
    PGBOUNCER_DEFAULT_POOL_SIZE: int
    PGBOUNCER_MAX_CONNECTIONS: int
    PGBOUNCER_MIN_CONNECTIONS: int
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

class ApiSettings(BaseSettings):
    hf_token: str
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

llm_settings = LLMSettings()
pg_settings = PGSettings()
api_settings = ApiSettings()