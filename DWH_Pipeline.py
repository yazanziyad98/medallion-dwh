from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, trim
from datetime import datetime
import os
from dotenv import load_dotenv


load_dotenv()

MINIO_ACCESS    = os.getenv("MINIO_ACCESS")
MINIO_SECRET    = os.getenv("MINIO_SECRET")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT")
MYSQL_USER      = os.getenv("MYSQL_USER")
MYSQL_PASSWORD  = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST      = os.getenv("MYSQL_HOST")
MYSQL_URL       = os.getenv("MYSQL_URL")

