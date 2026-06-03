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


spark = SparkSession.builder.appName("Gov_Pipeline") \
    .config("spark.jars", "/home/yazan/mysql-connector-j-9.5.0.jar") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,") \
    .config("spark.driver.extraClassPath", "/home/yazan/mysql-connector-j-9.5.0.jar") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts") \
    .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp/s3a") \
    .getOrCreate()

