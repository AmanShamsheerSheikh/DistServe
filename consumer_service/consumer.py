
import asyncio
import json
from discom.constants import JobStatus
from discom.queries import update_job, update_chunks
from db import init, get_consumer, get_db_pool, get_gpu_worker

async def consumer_requests():
    consumer = get_consumer()
    db_pool = get_db_pool()
    gpu_worker = get_gpu_worker()
    try:
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                    msg.key, msg.value, msg.timestamp)
            request = json.loads(msg.value.decode('utf-8'))
            await gpu_worker.token_generator.spawn.aio(
                source_text=request["source_text"],
                chunk_id=request["id"],
                document_id=request["document_id"],
                text_type=request["address"]["type"],
            )
            async with db_pool.acquire() as connection:
                await update_chunks(connection, JobStatus.RUNNING.value, request["id"], None, None)
                await update_job(connection, JobStatus.RUNNING.value, request["document_id"], None, None)
            await consumer.commit()      
    except Exception as e:
        print(f"Consumer loop crashed: {e}")
    finally:
        print("Stopping consumer...")
        await consumer.stop()
        await db_pool.close()


async def main():
    await init()
    await consumer_requests()

if __name__ == '__main__':
    asyncio.run(main())