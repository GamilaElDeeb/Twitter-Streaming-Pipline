import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder \
        .appName("myApp") \
        .enableHiveSupport() \
        .getOrCreate()
spark.conf.set("hive.metastore.uris", "thrift://localhost:5432")
spark.conf.set("spark.sql.sources.default", "parquet")
schema = StructType([
    StructField("tweet_id", StringType()),
    StructField("tweet_text", StringType()),
    StructField("created_at", StringType()),
    StructField("location", StringType()),
    StructField("language", StringType()),
    StructField("author_name", StringType()),
    StructField("author_verified", StringType()),
    StructField("public_metrics", StringType())
])

def extract_tweet_type(text):
    if "hiring" in text.lower() or "job alert" or "job_alert" in text.lower():
        return "Job Alert"
    else:
        return "Other"

def extract_work_location(text):
    if "remote" in text.lower():
        return "Remote"
    elif "work from home" in text.lower():
        return "Work From Home"
    elif "premise" in text.lower() or "office" in text.lower():
        return "On Office"
    else:
        return "Other"

tweet_type_udf = udf(extract_tweet_type, StringType())
work_location_udf = udf(extract_work_location, StringType())

tweet_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 7777) \
    .load()
tweet_df_string = tweet_df.selectExpr("CAST(value AS STRING)")
writeTweet = tweet_df_string.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("tweetquery") \
    .trigger(processingTime='2 seconds') \
    .start()
time.sleep(60)
df = spark.sql("select * from tweetquery")

rdd = df.rdd.flatMap(lambda row: row.value.replace('}{', '}###{').split('###'))
df = spark.read.json(rdd, schema=schema)
df = df.withColumn("public_metrics", split(df.public_metrics, ", ")) \
    .selectExpr("*", "public_metrics[0] as retweet_count", "public_metrics[1] as reply_count", "public_metrics[2] as like_count", "public_metrics[3] as quote_count") \
        .drop("public_metrics")
df = df.withColumn("retweet_count", regexp_extract(regexp_replace("retweet_count", "[^0-9]", ""), "\d+", 0).cast("integer"))
df = df.withColumn("reply_count", regexp_extract(regexp_replace("reply_count", "[^0-9]", ""), "\d+", 0).cast("integer"))
df = df.withColumn("like_count", regexp_extract(regexp_replace("like_count", "[^0-9]", ""), "\d+", 0).cast("integer"))
df = df.withColumn("quote_count", regexp_extract(regexp_replace("quote_count", "[^0-9]", ""), "\d+", 0).cast("integer"))
df = df.withColumn("tweet_type", tweet_type_udf(df.tweet_text)).withColumn("work_location", work_location_udf(df.tweet_text))
df = df.withColumn("location", when(df.location.isNull(), "UnDetermined").otherwise(df.location))
df = df.withColumn("year", year("created_at")) \
       .withColumn("month", month("created_at")) \
       .withColumn("day", dayofmonth("created_at")) \
       .withColumn("hour", hour("created_at"))
df.write.mode("append").partitionBy("year", "month", "day", "hour").parquet("/twitter-landing-data/extracted_tweet_data.parquet")
df.write.insertInto("fathy.landing_table", overwrite=True)
