from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

def process_stream(rdd):
    statuses = rdd.map(lambda x: json.loads(x[1])) \
        .map(lambda x: (x['type'], 1)) \
        .reduceByKey(lambda a, b: a + b)

    statuses.pprint()

if __name__ == "__main__":
    sc = SparkContext(appName="MeaslesStatusCounter")
    ssc = StreamingContext(sc, 5)

    brokers = "localhost:9092"
    topic = "measles_data"

    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])

    lines.foreachRDD(process_stream)

    ssc.start()
    ssc.awaitTermination()
