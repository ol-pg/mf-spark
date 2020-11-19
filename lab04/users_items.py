
# coding: utf-8

# In[108]:


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
sc = spark.sparkContext

spark


# In[ ]:


spark.conf.set("spark.sql.session.timeZone", "UTC")
#topic = spark.conf.get("spark.users_items.topic_name")
#offset = spark.conf.get("spark.users_items.offset")
outputDir = spark.conf.get("spark.users_items.output_dir_prefix")
inputDir = spark.conf.get("spark.users_items.input_dir")

update = spark.conf.get("spark.users_items.update", "1")
isUpdate = update if (update == "1") else "0"

#startingOffset = offset if (offset == "earliest") else '{"'+ topic + '":{"0":' + offset + '}}'


# In[3]:


#dfSparkBuy = spark.read.json( inputDir +'/buy')
dfSparkBuy = spark.read.json('/user/olga.pogodina/visits/buy')


# In[5]:


#dfSparkView = spark.read.json(inputDir + '/view')
dfSparkView = spark.read.json('/user/olga.pogodina/visits/view')


# In[9]:


from pyspark.sql.functions import *

buyMaxTs = dfSparkBuy.select(max("timestamp").alias("max_ts"))


# In[12]:


viewMaxTs = dfSparkView.select(max("timestamp").alias("max_ts"))


# In[99]:


totalMaxTs = buyMaxTs.union(viewMaxTs)                     .select(date_format(from_unixtime(max("max_ts") / 1000), "yyyyMMdd")                     .alias("max_ts")                     .cast('string'))


# In[100]:


maxDateDir = totalMaxTs.collect()[0][0]


# In[ ]:


# log.warn(s"Max date is $maxDateDir")


# In[65]:


dfBuyPrepared = dfSparkBuy.select("uid", "item_id")            .where("uid is not null")            .withColumn("item_id", lower(col("item_id")))            .withColumn('item_id', regexp_replace('item_id', '-', '_'))            .withColumn('item_id', regexp_replace('item_id', ' ', '_'))            .withColumn("item_id", concat(lit("buy_"), col("item_id")).alias("item_id"))            .groupBy("uid", "item_id")            .agg(count("*").alias("item_count"))


# In[67]:


dfViewPrepared = dfSparkView.select("uid", "item_id")            .where("uid is not null")            .withColumn("item_id", lower(col("item_id")))            .withColumn('item_id', regexp_replace('item_id', '-', '_'))            .withColumn('item_id', regexp_replace('item_id', ' ', '_'))            .withColumn("item_id", concat(lit("view_"), col("item_id")).alias("item_id"))            .groupBy("uid", "item_id")            .agg(count("*").alias("item_count"))


# In[70]:


dfTotal = dfBuyPrepared.union(dfViewPrepared)        .groupBy("uid")        .pivot("item_id")        .sum("item_count")        .na.fill(0)


# In[ ]:


if (isUpdate == 1):
    
    lastLoadDf = spark.read.parquet(outputDir + "/20200429").na.fill(0)
    usersPrev = lastLoadDf.select("uid")
    rowsNewOnly = dfTotal.join(lastLoadDf, "uid", "left_anti")
    vnewCols = set(rowsNewOnly.columns)
    oldCols = set(lastLoadDf.columns)


    def columnsAll(myCols, allCols):
        return map(lambda x: col(x) if x in myCols else lit(0).alias(x),allCols)

    cols = columnsAll(oldCols, oldCols)
    updatedDf = lastLoadDf.select(cols).union(rowsNewOnly.select(cols))
    
    updatedDf.write.parquet(outputDir + '/' + maxDateDir)
else:
    dfTotal.write.parquet(outputDir + '/' + maxDateDir)

