from confluent_kafka import Consumer, KafkaError

def consume_messages(topic):
    try:
        conf = {
            'bootstrap.servers': 'localhost:9093',
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(conf)
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, consumer reached end of topic
                    continue
                else:
                    # Error
                    print(f"Consumer error: {msg.error()}")
                    break
            else:
                # Message consumed successfully
                print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        # Graceful exit on Ctrl+C
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    topic = 'measles_data'
    consume_messages(topic)
