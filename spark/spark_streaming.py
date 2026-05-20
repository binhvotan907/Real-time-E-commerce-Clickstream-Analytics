from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window,
    count, approx_count_distinct, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

TOPIC_NAME = "clickstream"
BASE_PATH = "C:/Users/hokhi/Desktop/realtime-clickstream"

spark = (
    SparkSession.builder
    .appName("RealTimeClickstreamAnalytics")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Schema dữ liệu (YooChoose)
schema = StructType([
    StructField("session_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("category", IntegerType(), True),
])

# Đọc dữ liệu từ Kafka (Spark đóng vai trò Consumer, mỗi message Kafka = 1 click)
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON & event-time
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    # Sử dụng processing time thay cho timestamp gốc (dữ liệu lịch sử) để đảm bảo window và watermark hoạt động đúng trong môi trường demo
    .withColumn("event_time", current_timestamp())
    .withWatermark("event_time", "10 minutes") # spark chấp nhận dữ liệu trễ tối đa 10 phút
)

# ================= OUTPUT RAW DATA =================
parsed_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("path", f"{BASE_PATH}/output/parquet") \
    .option("checkpointLocation", f"{BASE_PATH}/checkpoint/parquet") \
    .start()

# METRIC 1 — TOP PRODUCT: Xác định sản phẩm được quan tâm nhiều nhất
(
    parsed_df
    .groupBy("item_id")
    .agg(count("*").alias("clicks"))
    .writeStream
    .outputMode("complete")
    .format("console")
    .trigger(processingTime="10 seconds")
    .option("truncate", "false")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoint/top_products")
    .start()
)

# METRIC 2 — ACTIVE SESSION (5 MIN): Số session đang hoạt động trong 5 phút gần nhất
(
    parsed_df
    .groupBy(window(col("event_time"), "5 minutes"))
    .agg(approx_count_distinct("session_id").alias("active_sessions"))
    .writeStream
    .outputMode("update")
    .format("console")
    .trigger(processingTime="10 seconds")
    .option("truncate", "false")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoint/active_sessions")
    .start()
)

# METRIC 3 — CLICKS PER 10 SECONDS: Đếm số click mỗi 10 giây
(
    parsed_df
    .groupBy(window(col("event_time"), "10 seconds"))
    .agg(count("*").alias("clicks"))
    .writeStream
    .outputMode("update")
    .format("console")
    .trigger(processingTime="10 seconds")
    .option("truncate", "false")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoint/clicks_10s")
    .start()
)

# METRIC 4 — TOP CATEGORY: Xác định danh mục sản phẩm được click nhiều nhất
(
    parsed_df
    .groupBy("category")
    .agg(count("*").alias("clicks"))
    .writeStream
    .outputMode("complete")
    .format("console")
    .trigger(processingTime="10 seconds")
    .option("truncate", "false")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoint/top_category")
    .start()
)

# METRIC 5 — BOUNCE SESSION (ONLY 1 CLICK): Phát hiện session chỉ có đúng 1 click
(
    parsed_df
    .groupBy(
        window(col("event_time"), "30 minutes"),
        col("session_id")
    )
    .agg(count("*").alias("clicks"))
    .filter(col("clicks") == 1)
    .writeStream
    .outputMode("update")
    .format("console")
    .trigger(processingTime="10 seconds")
    .option("truncate", "false")
    .option("checkpointLocation", f"{BASE_PATH}/checkpoint/bounce")
    .start()
)

spark.streams.awaitAnyTermination()
