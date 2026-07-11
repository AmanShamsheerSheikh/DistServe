from fastapi import Request

async def get_db_connection(request: Request):
    async with request.app.state.db_pool.acquire() as connection:
        yield connection

async def get_redis(request: Request):
    yield request.app.state.redis