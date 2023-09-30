# Twitter-Streaming-Pipline
#### Project Overview

------

The Spark Stream pipeline project is a data pipeline that retrieves, processes, stores, and analyzes Twitter data related to specific topics. It consists of four separate scripts that work  to collect tweets, store them on HDFS, create tables and perform transformations and aggregations. A Bash script coordinates the four scripts, and a Cron Tab triggers another Bash script periodically to ensure the pipeline's smooth running.

#### Architecture Overview

------

![](./imagesImages/236661118-abc7c938-da95-4015-84ed-9b7d3fcbbb22.png)

The project involves six data pipelines that work to collect and process live Twitter data. The data is first streamed through a TCP socket using a Python Twitter listener and fed into a Spark processing engine. From there, the processed data is stored in HDFS parquet format.

The stored data is then read into a Star Schema model that includes several Hive Dimension tables. Using a SparkSQL application, these dimensions are analyzed and used to create Hive Fact Tables. These Fact Tables provide the foundation for creating analytical insights and visualizations.



#### Tools and Technology Used

------

- Python 3

- Apache Spark (PySpark)

- Hive

- HDFS

#### project Explanation

------

The pipeline consists of five separate scripts, each explained as follows:

##### Twitter-Listener.py

- The first step in the pipeline is a script that connects to the Twitter API and retrieves recent tweets based on a given search query. 
- In this project, the topic of interest is "Data_Engineering." The script makes a GET request to retrieve the relevant data and converts it to a JSON object. 
- The data is then sent to the socket for further processing

##### Twitter-stream.py

- The Twitter_Stream script, written in PySpark, reads data from the socket stream. 
- The fields of interest are defined, and a schema is created using StructType. 
- The retrieved data includes tweet information such as ID, text, in_reply_to_user_id, created_at, public_metrics, and source. 
- User information, such as username , is also retrieved. The JSON data is converted to a DataFrame using the defined schema.

##### Hive_tables.hql

##### A HiveQL script responsible for creating the following tables:

- Tweet_base (Hive external table that holds all the stored data)
- Tweet_dim (Hive external table) that creates the first dimension table.
- Metrics_dim (Hive external table) that creates the second dimension table.
- Then the script continues to insert and update all the tables with the newly arrived data.

#### Tweet_fact.py

- A Spark SQL application that reads the data from the dimension tables, makes the transformations and aggregations required to perform the required analysis (measuring the publicity of the tweet topic at a specific point of time).
- Then, it stores the final fact table as an external Hive table on HDFS.
- The previous scripts are then coordinated using a bash script. A script is responsible for running the Twitter API and the Spark stream, and a Cron Tab is used to trigger another Bash script periodically, which checks if the Spark application is up and running, then continues to execute the rest of the pipeline.

##### Instructions

To use this pipeline, follow these instructions:

1. Add the following crontab command: */10 * * * * ./pipe2.bash >/dev/null 2>&1
2. Run the script named pipestart.bash. 
