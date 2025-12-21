from kafka import KafkaConsumer


consumer_1 = KafkaConsumer(
    "wiki-stream-events",
    bootstrap_servers="localhost:9092",
    group_id="group_1",
    auto_offset_reset="earliest"
)

consumer_2 = KafkaConsumer(
    "wiki-stream-events",
    bootstrap_servers="localhost:9092",
    group_id="group_2",
    auto_offset_reset="latest"
)

while True:
    records_1 = consumer_1.poll(timeout_ms=10)
    records_2 = consumer_2.poll(timeout_ms=10)

    # process consumer_1 messages
    for tp, msgs in records_1.items():
        for msg in msgs:
            print("group_1:", msg.value.decode())
            print("\n", "-----------------------------------", "\n")

    # process consumer_2 messages
    for tp, msgs in records_2.items():
        for msg in msgs:
            print("group_2:", msg.value.decode())
            print("\n", "-----------------------------------", "\n")
