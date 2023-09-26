import os
import subprocess


# Set up Twitter stream listener
twitter_listener_command = ['python3', 'twitter_listener.py']
twitter_listener_process = subprocess.Popen(twitter_listener_command)

#Create landing table if not exist
load_tweet_data_command = ['hive', '-f', 'load_tweet_data.hql']
subprocess.run(load_tweet_data_command, check=True)

# Stream data into Hive table
twitter_stream_command = ['/opt/spark-3.1.2-bin-hadoop3.2/bin/spark-submit', 'twitter_stream.py']
subprocess.run(twitter_stream_command, check=True)

# Create Hive dimension tables
hive_dimension_command = ['hive', '-f', 'create_dimension.hql']
subprocess.run(hive_dimension_command, check=True)

# Run Spark job
spark_job_command = ['/opt/spark-3.1.2-bin-hadoop3.2/bin/spark-submit', 'spark_job.py']
subprocess.run(spark_job_command, check=True)
