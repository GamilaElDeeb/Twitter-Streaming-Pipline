CREATE DATABASE IF NOT EXISTS dimension
LOCATION '/twitter-raw-data/dimension';

use dimension;

CREATE TABLE IF NOT EXISTS tweet_text_raw (
    tweet_id STRING,
    tweet_text STRING
)
PARTITIONED BY (year INT, month INT, day INT, hour INT);

set hive.exec.dynamic.partition.mode=nonstrict;


INSERT INTO TABLE tweet_text_raw PARTITION (year, month, day, hour)
SELECT tweet_id, tweet_text, year, month, day, hour
FROM fathy.landing_table;


CREATE TABLE IF NOT EXISTS author_raw (
    tweet_id STRING,
    author_name STRING,
    author_verified STRING,
    location STRING
)
PARTITIONED BY (year INT, month INT, day INT, hour INT);


INSERT INTO author_raw PARTITION (year, month, day, hour)
SELECT tweet_id, 
author_name, author_verified, location, year, month, day, hour
FROM Gamila.landing_table;

CREATE TABLE IF NOT EXISTS tweet_properties_raw (
    tweet_id STRING,
    work_location STRING,
    tweet_type STRING,
    created_at STRING,
    language STRING
)
PARTITIONED BY (year INT, month INT, day INT, hour INT);


INSERT INTO tweet_properties_raw PARTITION (year, month, day, hour)
       SELECT tweet_id, 
       work_location, 
       tweet_type, 
       created_at, 
       language,
       year,
       month,
       day,
       hour
FROM Gamila.landing_table;

CREATE TABLE IF NOT EXISTS metrics_raw (
    tweet_id STRING,
    retweet_count INT,
    reply_count INT,
    like_count INT,
    quote_count INT
)
PARTITIONED BY (year INT, month INT, day INT, hour INT);


INSERT INTO metrics_raw PARTITION (year, month, day, hour)
       SELECT tweet_id, 
       retweet_count, 
       reply_count, 
       like_count, 
       quote_count, 
       year, 
       month , 
       day , 
       hour 
FROM Gamila.landing_table;


