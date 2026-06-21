
from pyspark.sql.functions import col, when, trim
def safe_long(c):
    return when(trim(col(c)) == "", None).otherwise(col(c)).cast("long")
def safe_int(c):
    return when(trim(col(c)) == "", None).otherwise(col(c)).cast("int")
