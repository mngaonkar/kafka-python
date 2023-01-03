from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer

topic = ["recommend-requests"]

def process_message(msg):
    print("procesing message, topic =  ", msg.topic(), " partition = ", msg.partition())
    print("key = ", msg.key(), " value = ", msg.value())

try:
    c = Consumer({"bootstrap.servers": "portal.altbox.online:9092",
        "group.id": "recommend-consumer",
        "auto.offset.reset": "smallest"})
except Exception as e:
    print("error creating consumer, err = ", e)

try:
    c.subscribe(topic)
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None: continue

        if msg.error():
            print(msg.error())
            break

        process_message(msg)
except Exception as e:
    print("error processing message, err = ", e)


