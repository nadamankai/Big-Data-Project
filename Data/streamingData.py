
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import csv
from random import shuffle
from time import sleep

def generate_streaming_data(filename, delay=5):
    """
    Simulates a streaming data source by reading rows from a CSV file
    randomly and yielding a row every specified delay.

    Args:
        filename (str): Path to the CSV file containing data.
        delay (int, optional): Delay between yielding rows (in seconds). Defaults to 5.

    Yields:
        str: Each row from the CSV file (randomly shuffled)
    """

    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        data = list(reader)  # Read all rows into a list
        shuffle(data)  # Randomly shuffle the data for a stream-like behavior

    for row in data:
        yield row
        sleep(delay)  # Introduce delay between yielding rows

# Example usage
# for row in generate_streaming_data("shuffled_measles_2421.csv"):
#     print(row)  # Replace with your processing logic
#     # ... process the data row


def process_streaming_data(rdd):
    """
    Processes each batch of data received from the streaming source.

    Args:
        rdd: Spark RDD containing a batch of data rows
    """

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(rdd, schema=["col1", "col2", "col3"])  # Assuming your CSV schema

    # Your data processing logic here (e.g., filtering, aggregation)
    filtered_df = df.filter(col("col1") > 10)  # Example filtering

    # Print or store the processed data (replace with your logic)
    filtered_df.show()


# Spark Streaming configuration
spark_master = "bigdataproject-spark-1"  # Adjust based on your Spark cluster
app_name = "Streaming Data Processing"
batch_duration = 5  # Adjust based on your desired processing window

# Kafka consumer configuration
kafka_brokers = "localhost:9092"  # Replace with your Kafka broker address
topic_name = "your_topic_name"  # Replace with your Kafka topic


# Create Spark Streaming context
ssc = StreamingContext("bigdataproject-spark-1", app_name, batch_duration)

# Simulate streaming data source (replace with a real Kafka consumer later)
stream = ssc.sparkContext.parallelize([row for row in generate_streaming_data("shuffled_measles_2421.csv")])

# Process the streaming data
stream.foreachRDD(process_streaming_data)

# Start Spark Streaming application
ssc.start()
ssc.awaitTermination()