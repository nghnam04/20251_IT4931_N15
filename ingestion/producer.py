import json, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer
import csv

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

with open("twitter_validation.csv") as f:
    reader = csv.reader(f)
    for data in reader:
        producer.send('test_tweet', value = data)
        time.sleep(2)

