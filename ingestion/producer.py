import json, time
from kafka import KafkaProducer
import csv
from datetime import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

topic = 'social_stream'

with open("../data/twitter_validation.csv", encoding='utf-8') as f:
    reader = csv.reader(f)
    for row in reader:
        data  ={
            'id': row[0],
            'Topic': row[1],
            'Text': row[3],
            'event_time': datetime.now().isoformat()
        }
        producer.send(topic, value = data)
        time.sleep(2)