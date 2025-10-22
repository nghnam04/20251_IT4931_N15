import json, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer
import pandas as pd

producer = KafkaProducer(bootstrap_servers="kafka.default.svc.cluster.local:9092",
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

texts = pd.read_csv('data/twitter_training.csv')['Text'].tolist()

def gen_record(i):
    return {
        "post_id": i,
        "user_id": random.randint(1, 10000),
        "text": random.choice(texts),
        "lang": "en",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

i = 0
while True:
    rec = gen_record(i); i += 1
    producer.send("posts", rec)
    time.sleep(0.05)  # 20 msg/giây
