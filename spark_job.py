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

df_tweet_text = spark.sql("SELECT tweet_id, tweet_text FROM dimension.tweet_text_raw WHERE tweet_text IS NOT NULL")
df_tweet_properties = spark.sql("SELECT * FROM dimension.tweet_properties_raw")
df_tweet_author = spark.sql("SELECT * FROM dimension.author_raw")
df_tweet_metrics = spark.sql("SELECT * FROM dimension.metrics_raw")

df_tweet_properties_filtered = df_tweet_properties.filter(lower(df_tweet_properties.tweet_type) == 'job alert')
df_tweet_metrics_properties = df_tweet_metrics.select('tweet_id', 'retweet_count', 'like_count')
df_tweet_metrics_properties = df_tweet_properties_filtered.join(df_tweet_metrics_properties, 'tweet_id', 'inner')
df_tweet_text_filtered = df_tweet_text.select('tweet_id', 'tweet_text')
df_tweet_engagement = df_tweet_metrics_properties.join(df_tweet_text_filtered, 'tweet_id', 'inner')
df_tweet_engagement_hour = df_tweet_engagement.groupBy('tweet_type','language','work_location','hour','day','month','year').agg({'like_count': 'sum', 'retweet_count': 'sum'})

df_tweet_engagement_hour = df_tweet_engagement_hour.withColumnRenamed('sum(like_count)', 'like_count').withColumnRenamed('sum(retweet_count)', 'retweet_count')

spark.sql("CREATE DATABASE IF NOT EXISTS twitter_processed_data")
df_tweet_engagement_hour.write.mode('overwrite').saveAsTable('twitter_processed_data.tweet_engagement_hour_processed')
df_tweet_engagement.write.mode('overwrite').saveAsTable('twitter_processed_data.tweet_engagement_processed')

df_tweet_location_filtered = df_tweet_properties.filter(lower(df_tweet_properties.tweet_type) == 'job alert')
df_tweet_metrics_properties = df_tweet_metrics.select('tweet_id', 'retweet_count', 'like_count' , 'reply_count')
df_tweet_metrics_properties = df_tweet_location_filtered.join(df_tweet_metrics_properties, 'tweet_id', 'inner')
df_tweet_author_filtered = df_tweet_author.select('tweet_id','location')
df_tweet_author_filtered = df_tweet_author_filtered.join(df_tweet_metrics_properties, 'tweet_id', 'inner')

df_tweet_location_engagement = df_tweet_author_filtered.groupBy('location','hour', 'day', 'month', 'year').agg({
    'retweet_count': 'sum',
    'like_count': 'sum',
    'reply_count': 'sum',
    'tweet_type': 'count'
}).withColumnRenamed('count(tweet_type)', 'tweet_count').withColumnRenamed('sum(like_count)', 'likes')  \
.withColumnRenamed('sum(reply_count)', 'replies').withColumnRenamed('sum(retweet_count)', 'retweets')

df_tweet_location_engagement.write.mode('overwrite').saveAsTable('twitter_processed_data.tweet_location_processed')
