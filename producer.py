from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer

topic = "test-messages"
message = "test message from Python producer"

def delivery_report(err, msg):
    if err is not None:
        print("message delivery failed with error ", err)
    else:
        print("message delivered")

a = AdminClient({"bootstrap.servers": "147.182.230.45:9092"})
try:
    md = a.list_topics(timeout=10)
    print(md.brokers)

    p = Producer({"bootstrap.servers": "147.182.230.45:9092"})
    print("sending message ", message)
    p.produce(topic, message.encode('utf-8'), callback=delivery_report)
    p.flush(timeout=5)

except Exception as e:
    print(e)
