from pyspark.ml import PipelineModel
import re
import nltk
# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize
from kafka import KafkaConsumer
from json import loads
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import from_json, expr, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from EmptyStringFilter import EmptyStringFilter
# Establish connection to MongoDB
client = MongoClient('localhost', 27017)
db = client['bigdata_project'] 
collection = db['tweets'] 



# 1. Tạo SparkSession
def create_spark():
    spark = (
        SparkSession.builder
        .appName("SocialStreamSentiment")
        # .master("local[*]")  # bật nếu bạn test local
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark



def clean_text(text):
    if text is not None:
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
        return ''
    
clean_text_udf = udf(clean_text, StringType())

class_index_mapping = { 0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant" }


def main():
    spark = create_spark()

    # load model
    pipeline = PipelineModel.load("../logistic_regression_model_spark")

    message_schema = StructType([
        StructField("id", StringType(), True),
        StructField("Topic", StringType(), True),
        StructField("Text", StringType(), True),
        StructField("event_time", StringType(), True), 
    ])

    #Đọc stream từ Kafka 
    kafka_bootstrap = "localhost:9092"
    kafka_topic = "social_stream"

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # value là binary -> cast sang string
    json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_string")

    # Parse JSON
    parsed_df = (
        json_df
        .select(from_json(col("json_string"), message_schema).alias("data"))
        .select("data.*")
    )

    parsed_df = parsed_df.withColumn(
        'Text',
        clean_text_udf(col('Text'))
    )

    # Convert event_time (string) -> timestamp (nếu event_time có dạng ISO)
    parsed_df = parsed_df.withColumn(
        "event_time_ts",
        col("event_time").cast(TimestampType())
    )

    # Thêm sentiment_score, sentiment_label 
    with_sentiment_df = pipeline.transform(parsed_df)
    with_sentiment_df = with_sentiment_df.withColumn(
        "sentiment_label",
        expr(f"""
            CASE prediction
                WHEN 0.0 THEN 'Negative'
                WHEN 1.0 THEN 'Positive'
                WHEN 2.0 THEN 'Neutral'
                WHEN 3.0 THEN 'Irrelevant'
                ELSE 'Unknown'
            END
        """)
    )
    
    # Thêm metainfo để check latency
    # ingest_time: thời gian Spark nhận message
    # latency_ms: (ingest_time - event_time) tính bằng ms (nếu có event_time)
    with_latency_df = (
        with_sentiment_df
        .withColumn("ingest_time", current_timestamp())
        .withColumn(
            "latency_ms",
            (unix_timestamp(col("ingest_time")) - unix_timestamp(col("event_time_ts")))
            * 1000.0
        )
    )

    # Chọn các cột cần in/log
    result_df = with_latency_df.select(
        "id",
        "Topic",
        "Text",
        "event_time_ts",
        # "ingest_time",
        # "latency_ms",
        "sentiment_label"
    )

    # Ghi ra console (dùng để kiểm thử) 
    console_query = (
        result_df.writeStream
        .format("console")
        .outputMode("append")  # streaming từ Kafka thường dùng append
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", "./chk/social_stream_console")  # nhớ tạo thư mục nếu cần
        .start()
    )

    # ========== (OPTIONAL) Ghi ra file JSON local ==========
    # Bật nếu muốn lưu ra file để phân tích thêm
    # file_query = (
    #     result_df.writeStream
    #     .format("json")
    #     .option("path", "./output/social_stream")
    #     .option("checkpointLocation", "./chk/social_stream_file")
    #     .outputMode("append")
    #     .start()
    # )

    console_query.awaitTermination()
    # file_query.awaitTermination()


if __name__ == "__main__":
    main()
