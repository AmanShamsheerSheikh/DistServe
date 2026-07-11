
import asyncio
import json
from discom.constants import JobStatus
from discom.queries import update_job
import db

async def consumer_requests():
    consumer = db.get_consumer()
    db_pool = db.get_db_pool()
    try:
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                    msg.key, msg.value, msg.timestamp)
            request = json.loads(msg.value.decode('utf-8'))
            async with db_pool.acquire() as connection:
                await update_job(connection, JobStatus.RUNNING.value, request["id"], None, None)               
    except Exception as e:
        print(f"Consumer loop crashed: {e}")
    finally:
        print("Stopping consumer...")
        await consumer.stop()
        await db_pool.close()


async def main():
    await db.init_consumer()
    await consumer_requests()

if __name__ == '__main__':
    asyncio.run(main())