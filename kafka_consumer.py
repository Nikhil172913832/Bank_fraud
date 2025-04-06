import logging
from pyspark.sql.functions import udf, col
from pyspark.sql.types import MapType, StringType
import msgpack
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("FraudDetectionTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("consumer.log")],
)
log = logging.getLogger("kafka_consumer")
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()


def decode_msgpack(b):
    if b is not None:
        return msgpack.unpackb(b, raw=False)
    return {}
decode_udf = udf(decode_msgpack, MapType(StringType(), StringType()))
decoded_df = raw_df.withColumn("decoded", decode_udf(col("value")))
structured_df = decoded_df.selectExpr(
    "CAST(key AS STRING)",
    "decoded['transaction_id'] AS transaction_id",
    "CAST(decoded['timestamp'] AS DOUBLE) AS timestamp",
    "CAST(decoded['amount'] AS DOUBLE) AS amount",
    "decoded['sender'] AS sender",
    "decoded['receiver'] AS receiver",
    "decoded['device_id'] AS device_id",
    "decoded['source'] AS source"
)

query = structured_df.writeStream \
.format("console") \
.option("truncate", "false") \
.option("numRows", 20)\
.option("checkpointLocation", "/tmp/checkpoint_console") \
.start()
query.awaitTermination()



