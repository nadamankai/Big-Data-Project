from kafka import KafkaConsumer

def consume_messages(topic):
    try:
        consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')

        for message in consumer:
            print(f"Received message: {message.value.decode('utf-8')}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    topic = 'measles_data'
    consume_messages(topic)
