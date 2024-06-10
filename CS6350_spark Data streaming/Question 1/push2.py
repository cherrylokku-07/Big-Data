from pyspark.sql.functions import col, explode, count, current_timestamp, window
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, TimestampType
from pyspark.sql.functions import from_json, to_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import expr
import nltk
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
from nltk.chunk import ne_chunk
from newsapi import NewsApiClient
from kafka import KafkaProducer

spark = SparkSession.builder \
    .appName("KafkaStreamReader") \
    .getOrCreate()

kafka_broker = "localhost:9092"
topic = "topic1-events"
kafka_source_options = {
    "kafka.bootstrap.servers": kafka_broker,
    "subscribe": topic,
    "startingOffsets": "earliest" 
}

kafka_sink_topic = "topic2-events"
kafka_sink_options = {
    "bootstrap.servers": kafka_broker,
}

def extracted_ne(text_bytes):
    text = text_bytes.decode('utf-8')
    words = word_tokenize(text)
    pos_tags = pos_tag(words)
    ne_chunks = ne_chunk(pos_tags)
    named_entities = []
    for chunk in ne_chunks:
        if hasattr(chunk, 'label'):
            named_entities.append(''.join(c[0] for c in chunk))
    return named_entities

ne_udf = udf(extracted_ne, ArrayType(StringType()))

news_api_key = '6664af70dfcc4de2a745ec01bd7011c6'  
listener_bootstrap_servers = kafka_broker

try:
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_source_options) \
        .load()
    df = df.withColumn("named_entities", ne_udf(df["value"]))
    df = df.select(col("key"), explode(col("named_entities")).alias("named_entity"))
    ent_count = df.groupBy("named_entity") \
        .agg(count("*").alias("count"))

    news_api = NewsApiClient(api_key=news_api_key)
    kafka_producer = KafkaProducer(bootstrap_servers=[kafka_broker])

    def send_to_kafka(row):
        value = {"named_entity": row.named_entity, "count": row["count"]}
        kafka_producer.send(kafka_sink_topic, value)

    query = ent_count \
        .writeStream \
        .foreach(send_to_kafka) \
        .outputMode("update") \
        .start()

    query.awaitTermination(timeout=30)
finally:
    spark.stop()
