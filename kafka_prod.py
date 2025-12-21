from kafka import KafkaProducer
import json
from requests_sse import EventSource

url = "https://stream.wikimedia.org/v2/stream/recentchange"
headers = {
    "User-Agent": "WikiStreamLearning (shahar@example.com)",
    "Accept": "text/event-stream",
}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092"
)

with EventSource(url, headers=headers) as stream:
    for event in stream:
        if event.type != "message":
            continue

        producer.send("wiki-stream-events", value=event.data.encode("utf-8"))


producer.flush()
producer.close()
        

