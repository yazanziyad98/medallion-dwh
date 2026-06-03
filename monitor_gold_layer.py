from minio import Minio
from dotenv import load_dotenv
from datetime import datetime
import os


def monitor_gold_bucket():
    import mysql.connector
    load_dotenv()

    MINIO_ACCESS = os.getenv("MINIO_ACCESS")
    MINIO_SECRET = os.getenv("MINIO_SECRET")
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

    client = Minio(MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False
    )

    total_size_bytes = 0
    bucket_name = "dest.data.gold"

    objects = client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        total_size_bytes += obj.size

    total_size_mb = total_size_bytes / (1024 * 1024)
    bucket = next(b for b in client.list_buckets() if b.name == bucket_name)

    bucket_info = {
        "bucket_name": bucket_name,
        "creation_date": bucket.creation_date,
        "size_mb": round(total_size_mb, 2),
        "monitoring_date": datetime.now()
    }

    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database="datasource"
    )

    cursor = connection.cursor()

    insert_query = """
        INSERT INTO gold_bucket_monitoring (bucket_name, creation_date, size_mb, monitoring_date)
        VALUES (%s, %s, %s, %s)
"""

    cursor.execute(insert_query, (
        bucket_info["bucket_name"],
        bucket_info["creation_date"],
        bucket_info["size_mb"],
        bucket_info["monitoring_date"]
    ))

    connection.commit()

    cursor.close()
    connection.close()

    return bucket_info

if __name__ == "__main__": 
     monitor_gold_bucket()