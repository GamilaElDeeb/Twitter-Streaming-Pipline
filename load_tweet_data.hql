-- Create Gamila database if not exists
CREATE DATABASE IF NOT EXISTS Gamila;

-- Use Gamila database
USE Gamila;

-- Create landing_table table if not exists
CREATE TABLE IF NOT EXISTS landing_table (
   tweet_id string,
   tweet_text string,
   created_at string,
   location string,
   language string,
   author_name string,
   author_verified string,
   retweet_count string,
   reply_count string,
   like_count string,
   quote_count string,
   tweet_type string,
   work_location string,
   year int,
   month int,
   day int,
   hour int
) STORED AS PARQUET;



