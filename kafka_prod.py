from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092"
)

# msg = "hello kafka"
# msg_bytes = msg.encode("utf-8")

# producer.send("shahar_topic", value=msg_bytes)




while True:
    text = input()
    if text == "quit":
        break
    producer.send("shahar_topic", text.encode("utf-8"))

producer.flush()
producer.close()