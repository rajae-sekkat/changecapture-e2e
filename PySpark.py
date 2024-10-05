from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder \
    .appName("Kafka Integration Example") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark = SparkSession.builder.appName("TransactionMonitoring").getOrCreate()

# Kafka read stream
kafka_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "192.168.56.102:9092")\
    .option("subscribe", "cdc")\
    .load()

# Assuming transactions are in JSON format
schema = "transactionId STRING, userId STRING, amount DOUBLE, timestamp STRING, ..."

json_df = kafka_df.selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), schema).alias("data"))

# Filter large transactions
large_transactions_df = json_df.filter(col("data.amount") > 500)

# Write large transactions to the console or external storage
query = large_transactions_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
