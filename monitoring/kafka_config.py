from kafka import KafkaConsumer
consumer = KafkaConsumer('all-measles-rates.csv')
for msg in consumer:
     print (msg)