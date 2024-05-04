from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType

SCHEMA = StructType([
    StructField("index", StringType()),
    StructField("state", StringType()),
    StructField("year", StringType()),
    StructField("name", StringType()),
    StructField("type", StringType()),
    StructField("city", StringType()),
    StructField("county", StringType()),
    StructField("district", StringType()),
    StructField("enroll", IntegerType()),  # Assuming enrollment is a whole number
    StructField("mmr", IntegerType()),      # Assuming MMR is a whole number
    StructField("overall", StringType()),
    StructField("xrel", StringType()),
    StructField("xmed", StringType()),
    StructField("xper", StringType()),
])

spark = SparkSession.builder.appName("process_measles_data").getOrCreate()

# Read measles data from CSV file
df_measles = spark.read.csv("shuffled_measles_2421.csv", header=True, schema=SCHEMA)

# Optional processing (modify as needed)
# df_measles = df_measles.select(...)  # Select specific columns

# Write to console (temporary) or replace with a sink for storage
df_measles.writeStream.outputMode("append").format("console").start().awaitTermination()
