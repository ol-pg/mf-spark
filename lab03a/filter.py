
# coding: utf-8

# In[180]:


spark.stop


# In[1]:


import os
import sys
os.environ["PYSPARK_PYTHON"]='/opt/anaconda/envs/bd9/bin/python'
os.environ["SPARK_HOME"]='/usr/hdp/current/spark2-client'
os.environ["PYSPARK_SUBMIT_ARGS"]='--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7,org.elasticsearch:elasticsearch-spark-20_2.11:7.7.0,org.postgresql:postgresql:42.2.12,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 --num-executors 3 pyspark-shell'

spark_home = os.environ.get('SPARK_HOME', None)

sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))


# In[3]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("olpg").getOrCreate()
spark.conf.set('spark.filter.topic_name','lab03_input_data')
spark.conf.set('spark.filter.offset','earliest')
spark.conf.set('spark.filter.output_dir_prefix','visits')

sc = spark.sparkContext

spark


# In[ ]:


def kill_all():
    streams = SparkSession.builder.getOrCreate().streams.active
    if streams:
        for s in streams:
            desc = s.lastProgress["sources"][0]["description"]
            s.stop()
            print("Stopped {s}".format(s=desc))


# In[ ]:


kill_all()


# In[4]:


event = spark.read     .option("kafka.bootstrap.servers", 'spark-master-1:6667')     .option("subscribe", 'lab03_input_data')     .option("startingOffsets", """earliest""")     .format("kafka")     .load() 


# In[5]:


event.printSchema()
event.show(5)


# In[6]:


from pyspark.sql.functions import *


# In[7]:


json_doc = event.select(col("value").cast("string"))

json_doc.show(10,False)


# In[8]:


from pyspark.sql.types import *


# In[9]:


schema = 'array<struct<event_type:STRING,category:STRING,item_id:STRING,item_price:INTEGER,uid:STRING,timestamp:STRING>>'


# In[10]:


data = json_doc.withColumn('data', explode(from_json('value', schema)))                .select(*json_doc.columns, 'data.*')


# In[11]:


data = data.select("event_type","category","item_id","item_price","uid","timestamp")


# In[12]:


from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

udf1 = udf(lambda x: x[:-3],StringType())
data = data.withColumn('date',udf1('timestamp'))


# In[13]:


import pyspark.sql.functions as f

data = data.withColumn("date", f.from_unixtime("date", "yyyyMMdd"))


# In[14]:


data = data.withColumn("p_date",col("date"))


# In[15]:


data = data.select(col("event_type"), col("category"),                   col("item_id"), col("item_price"),                   col("uid"),col("timestamp"),
                   col("date").cast("string"), col("p_date"))


# In[16]:


data.show(2)


# In[155]:


dat_buy = data.where(f.col("event_type").like('buy'))


# In[167]:


db = dat_buy.repartitionByRange("p_date")


# In[168]:


print(db.rdd.getNumPartitions())


# In[156]:


dat_view = data.where(f.col("event_type").like('view'))


# In[169]:


dv = dat_view.repartitionByRange("p_date")


# In[170]:


print(dv.rdd.getNumPartitions())


# In[171]:


db.write.save('/user/olga.pogodina/visits/buy', format='json')


# In[176]:


dv.write.save('/user/olga.pogodina/visits/view', format='json')

