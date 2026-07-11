from settings import redis_settings
import redis.asyncio as redis


redisConnection: redis.Redis | None = None

async def init_redis():
    global redisConnection
    pool = redis.ConnectionPool(
        host=redis_settings.REDIS_HOST,
        port=redis_settings.REDIS_PORT,
        db=redis_settings.REDIS_DB,
        password=redis_settings.REDIS_PASSWORD,
        decode_responses=redis_settings.REDIS_DECODE_RESPONSES
    )
    redisConnection = redis.Redis(connection_pool=pool)

def get_redis():
    return redisConnection