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
from pyspark.sql.functions import from_json, expr, current_timestamp, unix_timestamp, when, sum
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from EmptyStringFilter import EmptyStringFilter
from pyspark.sql.functions import count, window
from pymongo import UpdateOne
from pyspark.sql.functions import to_date, current_date

# Establish connection to MongoDB
client = MongoClient('localhost', 27017)
db = client['bigdata_project'] 


def write_windowed_to_mongo(df, batch_id):
    # Chuyển đổi sang Pandas và xử lý cột window
    # Vì window là struct, ta cần convert để nó thân thiện với Mongo
    pdf = df.toPandas()
        
    if pdf.empty:
        return

    collection = db["windowed_result"]

    operations = []
    
    for _, row in pdf.iterrows():
        # Lấy thời gian bắt đầu và kết thúc của window
        window_start = row['window_start']
        window_end = row['window_end']
        
        # Tiêu chí định danh duy nhất (Unique Key)
        # Một record là duy nhất nếu trùng: Start Time + topic + Sentiment
        filter_criteria = {
            "window_start": window_start,
            "topic": row["topic"],
        }
        
        # Dữ liệu cập nhật
        update_data = {
            "$set": {
                "topic": row["topic"],
                "window_start": window_start,
                "window_end": window_end,
                "total_mentions": int(row["total_mentions"]), # Đảm bảo là kiểu int
                "positive": int(row["positive"]),
                "neutral": int(row["neutral"]), 
                "negative": int(row["negative"]),
                "sentiment_score": float(row["sentiment_score"]),
            }
        }
        
        operations.append(UpdateOne(filter_criteria, update_data, upsert=True))

    if operations:
        collection.bulk_write(operations)
    
    
# 1. Tạo SparkSession
def create_spark():
    spark = (
        SparkSession.builder
        .appName("SocialStreamSentiment")
        .getOrCreate()
    )
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
        StructField("topic", StringType(), True),
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
    
    df = with_sentiment_df.select(
        "topic",
        "event_time_ts",
        "sentiment_label"
    )

    result_df = (
        df
        .withWatermark("event_time_ts", "15 minutes")
        .groupBy(
            window(col("event_time_ts"), "5 minutes"),
            col("topic")
        )
        .agg(
            sum(when(col("sentiment_label") == "Positive", 1).otherwise(0)).alias("positive"),
            sum(when(col("sentiment_label") == "Neutral", 1).otherwise(0)).alias("neutral"),
            sum(when(col("sentiment_label") == "Negative", 1).otherwise(0)).alias("negative")
        )
        .withColumn(
            "total_mentions",
            col("positive") + col("neutral") + col("negative")
        )
        .withColumn(
            "sentiment_score",
            expr("""
                CASE
                    WHEN total_mentions = 0 THEN 0
                    ELSE (positive - negative) / total_mentions
                END
            """)
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .select(
            "window_start",
            "window_end",
            "topic",
            "total_mentions",
            "positive",
            "neutral",
            "negative",
            "sentiment_score"
        )
    )


    # Ghi ra console (dùng để kiểm thử) 
    # console_query = (
    #     result_df.writeStream
    #     .format("console")
    #     .outputMode("append")  # streaming từ Kafka thường dùng append
    #     .option("truncate", False)
    #     .option("numRows", 20)
    #     .option("checkpointLocation", "./chk/social_stream_console")  # nhớ tạo thư mục nếu cần
    #     .start()
    # )


    mongo_windowed_query = (
        result_df.writeStream
        .foreachBatch(write_windowed_to_mongo)
        .outputMode("update")
        .option("checkpointLocation", "./chk/windowed_sentiment")
        .start()
    )
    mongo_windowed_query.awaitTermination()

    client.close()
    # console_query.awaitTermination()


if __name__ == "__main__":
    main()
