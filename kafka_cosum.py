from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "shahar_topic",
    bootstrap_servers="localhost:9092",
    group_id="my-group",
    auto_offset_reset="earliest"
)

for message in consumer:
    print(message.value.decode("utf-8"))
