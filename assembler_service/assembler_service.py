
import asyncio
import json
from typing import List
from docx import Document
from discom.constants import JobStatus
from discom.queries import get_all_chunks, update_job
from db import get_consumer, get_db_pool, get_s3, init_db
from discom.constants import ChunkRecord
import io
import json
from settings import s3_settings

def download_from_s3(s3_client, bucket: str, key: str) -> io.BytesIO:
    buffer = io.BytesIO()
    s3_client.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    return buffer


async def reassemble_document(chunks, s3_key, s3_client, bucket: str):
    buffer = await asyncio.to_thread(download_from_s3, s3_client, bucket, s3_key)
    doc = Document(buffer)
    paragraphs = [chunk for chunk in chunks if chunk.address["type"] == "paragraph"]
    tables = [chunk for chunk in chunks if chunk.address["type"] == "table"]

    for para in paragraphs:
        doc.paragraphs[para.address["paragraph_index"]].text = para.result

    for table in tables:
        selected_table = doc.tables[table.address["table_index"]]
        cell_ids = table.address["cell_ids"]
        translated_cells = json.loads(table.result)

        if len(translated_cells) != len(cell_ids):
            # logger.error(f"Cell count mismatch for table_index={table.address['table_index']}, skipping")
            continue

        for (row_idx, col_idx), translated_text in zip(cell_ids, translated_cells):
            selected_table.rows[row_idx].cells[col_idx].text = translated_text

    return doc

async def assemble_requests():
    consumer = get_consumer()
    pool = get_db_pool()
    s3 = get_s3()
    try:
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                    msg.key, msg.value, msg.timestamp)
            request = json.loads(msg.value.decode('utf-8'))
            document_id = request["document_id"]
            async with pool.acquire() as connection:
                chunks: List[ChunkRecord] = await get_all_chunks(connection, document_id)
            translated_doc = await reassemble_document(chunks, document_id, s3, s3_settings.S3_BUCKET)
            output_buffer = io.BytesIO()
            translated_doc.save(output_buffer)
            output_buffer.seek(0)
            s3_result_key = "translated/" + str(document_id)
            await asyncio.to_thread(s3.upload_fileobj, output_buffer, s3_settings.S3_BUCKET, s3_result_key)
            async with pool.acquire() as connection:
                await update_job(connection, JobStatus.DONE.value, document_id, s3_result_key)
    except Exception as e:
        print(f"Consumer loop crashed: {e}")
    finally:
        print("Stopping consumer...")
        await consumer.stop()
        await pool.close()


async def main():
    await init_db()
    await assemble_requests()

if __name__ == '__main__':
    asyncio.run(main())