import io
import os
from dotenv import load_dotenv
from minio import Minio


def create_minio_client():
    """Создаем клиент для взаимодействия с объектным хранилищем"""
    load_dotenv()
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    client = Minio(endpoint, access_key, secret_key, secure=False)
    return client


def upload_to_minio(client, obj, bucket_name, object_name):
    """Загружаем python-объект в хранилище без сохранения на локальном диске"""
    stream_data = io.BytesIO()
    obj.to_file(stream_data, driver="GeoJSON")
    stream_data.seek(0)
    result = client.put_object(
        bucket_name,
        object_name,
        stream_data,
        length=-1,
        part_size=10 * 1024 * 1024,
        content_type="application/geo+json",
    )
    print(f"created {result.object_name} object")
