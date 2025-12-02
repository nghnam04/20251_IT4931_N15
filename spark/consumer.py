from pyspark.ml import PipelineModel
import re
import nltk

from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Establish connection to MongoDB
client = MongoClient('localhost', 27017)
db = client['bigdata_project'] 
collection = db['tweets'] 

# Download stopwords
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

#  Initialize Spark session
spark = SparkSession.builder \
    .appName("classify tweets") \
    .getOrCreate()

pipeline = PipelineModel.load("classify_model.pkl") # example model, not provided yet

def clean_text(text):
    if text is None:
        # Remove links starting with https://, http://, www., or containing .com
        text = re.sub(r'https?://\S+|www\.\S+|\.com\S+|youtu\.be/\S+', '', text)

        # Remove words starting with # or @
        text = re.sub(r'(@|#)\w+', '', text)

        # Convert to lowercase
        text = text.lower()

        # Remove non-alphanumeric characters
        text = re.sub(r'[^a-zA-Z\s]', '', text)

        # Remove extra whitespaces
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    else:
        return ""

class_index_mapping = {0: 'negative', 1: 'neutral', 2: 'positive'}

# Set up Kafka consumer
consumer = KafkaConsumer(
    'test_tweet',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

for message in consumer:
    tweet_data = message.value[-1] # get the text
    preprocessed_text = clean_text(tweet_data)

    data = [(preprocessed_text, )]
    data = spark.createDataFrame(data, ["text"])

    processed_validation = pipeline.transform(data)
    prediction = processed_validation.collect()[0]['prediction']

    print("-> Tweet:", tweet_data)
    print("-> preprocessed_tweet : ", preprocessed_text)
    print("-> Predicted Sentiment:", prediction)
    print("-> Predicted Sentiment classname:", class_index_mapping[int(prediction)])

    tweet_doc = {
        "tweet": tweet,
        "prediction": class_index_mapping[int(prediction)]
    }

    # Insert document into MongoDB collection
    collection.insert_one(tweet_doc)

    print("-"*50)

