import sys
import boto3
from discom.settings import S3Settings

s3_settings = S3Settings()


def download_translated(document_id: str, output_path: str = "output.docx"):
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_settings.S3_HOST,
        aws_access_key_id=s3_settings.MINIO_ROOT_USER,
        aws_secret_access_key=s3_settings.MINIO_ROOT_PASSWORD,
    )
    key = f"translated/{document_id}"
    s3.download_file(s3_settings.S3_BUCKET, key, output_path)
    print(f"Downloaded s3://{s3_settings.S3_BUCKET}/{key} -> {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python download_docx.py <document_id> [output_path]")
        sys.exit(1)

    document_id = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output_0.docx"
    download_translated(document_id, output_path)