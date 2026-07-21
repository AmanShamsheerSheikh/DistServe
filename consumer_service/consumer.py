
import asyncio
import json
from discom.constants import JobStatus, ChunkRecord
from discom.queries import update_job, update_chunks, get_chunk
from db import init, get_consumer, get_db_pool, get_gpu_worker


async def consumer_requests():
    consumer = get_consumer()
    db_pool = get_db_pool()
    gpu_worker = get_gpu_worker()
    try:
        async for msg in consumer:
            try:
                request = json.loads(msg.value.decode('utf-8'))
                async with db_pool.acquire as connection:
                    chunk: ChunkRecord = await get_chunk(connection, request["chunk_id"])
                if chunk is None:
                    print(f"Chunk {request["chunk_id"]} not found, discarding message")
                    await consumer.commit()
                    continue

                if chunk.status == JobStatus.DONE.value:
                    print(f"Chunk {chunk.id} already DONE, skipping redelivery")
                    await consumer.commit()
                    continue

                await gpu_worker.token_generator.spawn.aio(
                    source_text=chunk.source_text,
                    chunk_id=chunk.id,
                    document_id=chunk.document_id,
                    text_type=chunk.address["type"],
                )
                async with db_pool.acquire() as connection:
                    await update_chunks(connection, JobStatus.RUNNING.value, chunk.id, None, None)
                    await update_job(connection, JobStatus.RUNNING.value, chunk.document_id, None, None)
                await consumer.commit()
            except Exception as e:
                print(f"Failed to process message at offset {msg.offset}: {e}")
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