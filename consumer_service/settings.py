

from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int
    REDIS_PASSWORD: str | None
    REDIS_DECODE_RESPONSES: bool
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

class KafkaSettings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVER: str
    KAFKA_TOPIC: str
    KAFKA_GROUP_ID: str
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

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

pg_settings = PGSettings()
redis_settings = RedisSettings()
kafka_settings = KafkaSettings()