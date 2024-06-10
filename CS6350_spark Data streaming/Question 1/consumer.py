import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, count, window
from pyspark.sql.types import ArrayType, StringType
from nltk import pos_tag, ne_chunk
from nltk.tokenize import word_tokenize
from pyspark.sql.functions import udf
import newclient
from newclient import topic1, topic2
#from newsclient import topic1, topic2  # Importing Kafka topics from newsclient.py

# Download the required resources for NER
nltk.download('punkt')
nltk.download('maxent_ne_chunker')
nltk.download('words')

topic1 = "topic1-events"
topic2 = "topic2-events"

def extract_named_entities(text):
    words = word_tokenize(text)
    tagged_words = pos_tag(words)
    named_entities = ne_chunk(tagged_words)
    return [ne for ne in named_entities if hasattr(ne, 'label')]

spark = SparkSession.builder \
    .appName("ConsumerFile") \
    .getOrCreate()

# Create a streaming DataFrame that reads from the Kafka topic
df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', topic1) \
    .load() \
    .selectExpr('CAST(value AS STRING) as message', 'timestamp as message_timestamp')

# Use the UDF to extract named entities
extract_named_entities_udf = udf(extract_named_entities, ArrayType(StringType()))
df_with_entities = df.withColumn('named_entities', extract_named_entities_udf('message'))

# Add a watermark of 1 hour to handle late data
df_with_entities = df_with_entities.withWatermark("message_timestamp", "1 hour")

# Explode the named entities and count occurrences
words = df_with_entities.select(explode('named_entities').alias('named_entities'), "message_timestamp")
word_count = words.groupBy('named_entities', window('message_timestamp', '1 hour')).agg(count('*').alias('count'))

# Create a streaming DataFrame that writes the word count to another Kafka topic
word_count.selectExpr("to_json(struct(*)) AS value") \
    .writeStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('topic', topic2) \
    .option('checkpointLocation', '/Users/charanlokku/Downloads/checkpoints') \
    .start() \
    .awaitTermination()
