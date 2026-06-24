from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, trim
from datetime import datetime
import os
from dotenv import load_dotenv
from safe_transform import safe_long, safe_int

load_dotenv()

MINIO_ACCESS    = os.getenv("MINIO_ACCESS")
MINIO_SECRET    = os.getenv("MINIO_SECRET")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT")
MYSQL_USER      = os.getenv("MYSQL_USER")
MYSQL_PASSWORD  = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST      = os.getenv("MYSQL_HOST")
MYSQL_URL       = os.getenv("MYSQL_URL")


spark = SparkSession.builder.appName("DWH_Pipeline") \
    .config("spark.jars", "/home/yazan/mysql-connector-j-9.5.0.jar") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1") \
    .config("spark.driver.extraClassPath", "/home/yazan/mysql-connector-j-9.5.0.jar") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://dest.data.gold/") \
    .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.SimplifyCasts") \
    .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp/s3a") \
    .getOrCreate()



 


def read_table(table, partition_col, partitions_num):
    """
    Not Specifying Partitions will read the entire Table into one partition
    hence all the data is loaded into ram at once when invoking a job;
    which might crash.
    """
    bound = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", MYSQL_URL) \
        .option("fetchsize", "5000") \
        .option("dbtable", f"""(SELECT min({partition_col}) low_bound ,
                                max({partition_col}) as up_bound
                        FROM datasource.{table}) as tbl""") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .load().collect()[0]

    if bound[0] is None or bound[1] is None:
        raise ValueError(f"Table '{table}' appears to be empty — "
                         f"MIN/MAX on '{partition_col}' returned NULL. "
                         f"Aborting to avoid a full-scan read.")

    df = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", MYSQL_URL) \
        .option("dbtable", table) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("partitionColumn", partition_col) \
        .option("lowerBound", bound[0]) \
        .option("upperBound", bound[1]) \
        .option("numPartitions", partitions_num) \
        .option("fetchsize", "5000") \
        .load()
    return df



def write_objects(destination, bucket, entity, df, table):
    date = datetime.now()
    timestamp = date.strftime("%Y%m%d_%H%M%S")
    path = f"s3a://{bucket}/{entity}/{table}/{date.year}/{date.month}/{date.day}"

    if destination.lower() == 'staging':
        df.write.parquet(f"{path}/{table}_{timestamp}")
    elif destination.lower() == 'dwh':
        df.writeTo(f"iceberg.{entity}.{table}.`{date.year}`.`{date.month}`.`{date.day}`").createOrReplace()
    else:
        raise ValueError(f"Unknown destination '{destination}'. Expected 'staging' or 'dwh'.")
    return df



def filter_by_national_number(table):
    return Individual_info_stg[['National_Number']].join(table, "National_Number", "inner")



wages                        = read_table(table="wages",              partition_col="SSN", partitions_num=25)
insured_transaction          = read_table(table="insured_transaction",   partition_col="SSN", partitions_num=20)
insured_information          = read_table(table="insured_information",         partition_col="SSN", partitions_num=20)
insured_wage                 = read_table(table="insured_wage",         partition_col="SSN", partitions_num=20)
individual_information              = read_table(table="individual_info",         partition_col="Birth_Date",             partitions_num=30)


individual_info_df = individual_information \
    .withColumn("National_Number",        safe_long("National_Number")) \
    .withColumn("Gender",                 safe_int("Gender")) \
    .withColumn("Religion_Code",          safe_int("Religion_Code")) \
    .withColumn("Social_Status_Code",     safe_int("Social_Status_Code")) \
    .withColumn("Birth_Country_Code",     safe_int("Birth_Country_Code")) \
    .withColumn("Birth_Governorate_Code", safe_int("Birth_Governorate_Code")) \
    .withColumn("Birth_Liwa_Code",        safe_long("Birth_Liwa_Code")) \
    .withColumn("Father_National_Number", safe_long("Father_National_Number")) \
    .withColumn("Mother_National_Number", safe_long("Mother_National_Number"))

insured_info_df = individual_info_df \
    .withColumn("National_Number", safe_long("National_Number"))

wages_df = wages \
    .withColumn("National_Number", safe_long("National_Number"))

insured_wage_df = insured_wage \
    .withColumn("SSN", safe_long("SSN"))

insured_transaction_df = insured_transaction \
    .withColumn("SSN", safe_long("SSN"))




Individual_info_stg     = write_objects('staging', bucket='dest.data', entity='insurance', df=individual_info_df,  table="individual_info")
insured_info_stg          = write_objects('staging', bucket='dest.data', entity='insurance',  df=insured_info_df,          table="insured_info")
wages_stg              = write_objects('staging', bucket='dest.data', entity='wages',  df=wages_df,              table="wages")
insured_wage_stg        = write_objects('staging', bucket='dest.data', entity='wages',  df=insured_wage_df, table="insured_wage")
insured_transaction_stg   = write_objects('staging', bucket='dest.data', entity='wages',  df=insured_transaction_df,   table="insured_transaction")



wages_stg_nat  = filter_by_national_number(wages_stg)
insured_info_nat  = filter_by_national_number(insured_info_stg)


Individual_info_stg.createOrReplaceTempView('Individual_info_stg')

Individual_info_dip = spark.sql("""
    SELECT *,
           CASE WHEN Passport_Number LIKE '0000%' THEN 1 ELSE 0 END AS IS_Diplomat
    FROM Individual_info_stg
""")


dim_country       = spark.sql("SELECT DISTINCT Birth_Country_Code, Birth_Country FROM Individual_info_stg")
Individual_info_dip = Individual_info_dip.drop('Birth_Country')



# ────────────────── DWH writes ─────────────────────────────────────────

Individual_info_dwh         = write_objects('dwh', bucket='dest.data.gold', entity='insurance',      df=Individual_info_dip,        table="individual_information")
insured_info_dwh          = write_objects('dwh', bucket='dest.data.gold', entity='insurance',        df=insured_info_nat,          table="insured_information")
salaries_dwh              = write_objects('dwh', bucket='dest.data.gold', entity='wages',        df=wages_stg_nat,          table="salaries")
insured_yearly_salary_dwh = write_objects('dwh', bucket='dest.data.gold', entity='wages',        df=insured_wage_stg,           table="insured_wage")
insured_transaction_dwh   = write_objects('dwh', bucket='dest.data.gold', entity='wages',        df=insured_transaction_stg,   table="insured_transaction")
dim_country_dwh           = write_objects('dwh', bucket='dest.data.gold', entity='dimensions', df=dim_country,               table="dim_country")