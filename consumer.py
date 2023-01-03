from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer

c = Consumer({"bootstrap.servers": "portal.altbox.online:9092"})


