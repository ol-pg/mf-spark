
# coding: utf-8

# In[49]:


# spark.stop()


# In[50]:


# import os
# import sys
# os.environ["PYSPARK_PYTHON"]='/opt/anaconda/envs/bd9/bin/python'
# os.environ["SPARK_HOME"]='/usr/hdp/current/spark2-client'
# os.environ["PYSPARK_SUBMIT_ARGS"]='--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7,org.elasticsearch:elasticsearch-spark-20_2.11:7.7.0,org.postgresql:postgresql:42.2.12,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 --num-executors 3 pyspark-shell'

# spark_home = os.environ.get('SPARK_HOME', None)

# sys.path.insert(0, os.path.join(spark_home, 'python'))
# sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))


# In[51]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("olpg").getOrCreate()

# spark.conf.set('spark.filter.topic_name', 'lab03_input_data')
# spark.conf.set('spark.filter.offset', 'earliest')
# spark.conf.set('spark.filter.output_dir_prefix', '/user/olga.pogodina/visits')

sc = spark.sparkContext

spark


# In[52]:


spark.conf.set("spark.sql.session.timeZone", "Europe/London")


# In[53]:


raw_kafka = spark.readStream.format("kafka")      .option("kafka.bootstrap.servers", "spark-master-1:6667")      .option("subscribe", "olga_pogodina")      .option("startingOffsets", """earliest""").load()


# In[54]:


from pyspark.sql.types import *
from pyspark.sql.functions import *


# In[55]:


import pyspark.sql.functions as f


# In[56]:


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


# In[63]:


aggDF = raw_kafka.select(f.from_json(f.col("value").cast("string"), schema).alias("json"))            .select("json.*")            .withColumn("eventTime", f.from_unixtime(f.col("timestamp") / 1000))            .groupBy(f.window(f.col("eventTime"), "1 hour").alias("ts"))            .agg(f.sum(f.when(f.col("uid").isNotNull(), 1)).alias("visitors"),                 f.sum(f.when(f.col("event_type") == "buy", f.col("item_price"))).alias("revenue"),                 f.sum(f.when(f.col("event_type") == "buy", 1)).alias("purchases"))            .select(f.unix_timestamp(f.col("ts").getField("start")).alias("start_ts"),                    f.unix_timestamp(f.col("ts").getField("end")).alias("end_ts"),                    f.col("revenue"),                    f.col("visitors"),                    f.col("purchases"),                    (f.col("revenue") / f.col("purchases")).alias("aov"))


# In[ ]:


kafkaOutput = aggDF.select(f.to_json(f.struct(f.col("*"))).alias("value"))            .writeStream            .format("kafka")            .option("kafka.bootstrap.servers", '10.0.0.5:6667')            .option("topic", 'olga_pogodina_lab03b_out')            .outputMode("update")            .trigger(Trigger.ProcessingTime("5 seconds"))            .start()

