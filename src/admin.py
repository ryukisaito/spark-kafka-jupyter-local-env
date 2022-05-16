from confluent_kafka import Producer
import socket
from confluent_kafka.admin import AdminClient, NewTopic


def create_topics():
    a = AdminClient({'bootstrap.servers': 'localhost'})
    new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1)
                  for topic in ["topic1", "topic2"]]
    fs = a.create_topics(new_topics)
