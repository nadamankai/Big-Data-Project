from kafka import KafkaProducer
import csv


def produce_messages(file_path, topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9093')

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            producer.send(topic, value=row['type'].encode('utf-8'))

    producer.close()


if __name__ == "__main__":
    file_path = '../Data/shuffled_measles_2421.csv'
    topic = 'measles_data'
    produce_messages(file_path, topic)
