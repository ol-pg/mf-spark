
# coding: utf-8

# In[42]:


# spark.stop()


# In[1]:


# import os
# import sys
# os.environ["PYSPARK_PYTHON"]='/opt/anaconda/envs/bd9/bin/python'
# os.environ["SPARK_HOME"]='/usr/hdp/current/spark2-client'
# os.environ["PYSPARK_SUBMIT_ARGS"]='--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7,org.elasticsearch:elasticsearch-spark-20_2.11:7.7.0,org.postgresql:postgresql:42.2.12,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 --num-executors 3 pyspark-shell'

# spark_home = os.environ.get('SPARK_HOME', None)

# sys.path.insert(0, os.path.join(spark_home, 'python'))
# sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))


# In[2]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("olpg").getOrCreate()

# spark.conf.set('spark.filter.topic_name', 'lab03_input_data')
# spark.conf.set('spark.filter.offset', 'earliest')
# spark.conf.set('spark.filter.output_dir_prefix', '/user/olga.pogodina/visits')

sc = spark.sparkContext

spark


# In[3]:


spark.conf.set("spark.sql.session.timeZone", "Europe/London")


# In[41]:


raw_kafka = spark.readStream.format("kafka")      .option("kafka.bootstrap.servers", "spark-master-1:6667")      .option("subscribe", "olga_pogodina")      .option("startingOffsets", """earliest""").load()


# In[9]:


from pyspark.sql.types import *
from pyspark.sql.functions import *


# In[17]:


import pyspark.sql.functions as f


# In[25]:


schema = StructType(
      [
        StructField("event_type", StringType()),
        StructField("category", StringType()),
        StructField("item_id", StringType()),
        StructField("item_price", IntegerType()),
        StructField("uid", StringType()),
        StructField("timestamp", LongType())
      ]
    )


# In[30]:


parsedSdf = raw_kafka.select(col("value").cast(StringType()).alias("json"))                      .select(from_json(col("json"), schema).alias("data"))                      .select("data.*")                      .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))


# In[40]:


stream_data = parsedSdf.withWatermark("timestamp", "1 hour")                      .groupBy(window(col("timestamp"), "1 hour", "5 seconds").alias("window_tf"))                      .agg(sum(when(col("event_type") == lit("buy"), col("item_price"))).alias("revenue"),                           count(when(col("uid").isNotNull(), col("uid"))).alias("visitors"),                           count(when(col("event_type") == lit("buy"), col("event_type"))).alias("purchases"),                           avg(when(col("event_type") == lit("buy"), col("item_price"))).alias("aov"))                      .select(col("window_tf").getField("start").cast("long").alias("start_ts"),                              col("window_tf").getField("end").cast("long").alias("end_ts"),                              col("revenue"), col("visitors"),                              col("purchases"), col("aov"))


# In[ ]:


kafkaOutput = stream_data.select(f.to_json(f.struct(f.col("*"))).alias("value"))                         .writeStream                         .format("kafka")                         .option("kafka.bootstrap.servers", '10.0.0.5:6667')                         .option("topic", 'olga_pogodina_lab03b_out')                         .outputMode("update").start()

