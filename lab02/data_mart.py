
# coding: utf-8

# In[1]:


import os
import sys
os.environ["PYSPARK_PYTHON"]='/opt/anaconda/envs/bd9/bin/python'
os.environ["SPARK_HOME"]='/usr/hdp/current/spark2-client'
os.environ["PYSPARK_SUBMIT_ARGS"]='--packages org.elasticsearch:elasticsearch-spark-20_2.11:7.7.0,org.postgresql:postgresql:42.2.12,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3 --num-executors 3 pyspark-shell'

spark_home = os.environ.get('SPARK_HOME', None)

sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))


# In[2]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("opogodin").getOrCreate()
sc = spark.sparkContext

spark


# In[9]:


from pyspark.sql.functions import *


# ### Логи посещения веб-сайтов: hdfs

# - uid – уникальный идентификатор пользователя, тот же, что и в базе с информацией о клиенте (в Cassandra),
# - массив visits c некоторым числом пар (timestamp, url), где timestamp – unix epoch timestamp в миллисекундах, url - строка.
# 
# В этом датасете не все записи содержат uid. Это означает, что были покупатели, еще пока не идетифицированные и не внесенные в базу данных клиентов. Покупки и просмотры таких покупателей можно игнорировать в этом задании.

# In[3]:


get_ipython().system('hdfs dfs -ls /mf-labs/laba02/weblogs.json')


# In[4]:


df = spark.read.format("json").load('/mf-labs/laba02/weblogs.json')


# In[5]:


from pyspark.sql.functions import explode

df = df.select("uid",explode(df.visits).alias("visits"))


# In[6]:


df = df.select("uid",df.visits.timestamp.alias("timestamp"), df.visits.url.alias("url"))   


# In[7]:


df = df.filter(df.uid.isNotNull())


# In[10]:


df_url = df.withColumn("furl", expr("parse_url(url, 'HOST')"))
df_url = df_url.withColumn('furl', regexp_replace('furl', 'www.', ''))


# In[11]:


df_url.show(5)


# ### Логи посещения интернет-магазина: elastic

# - uid – уникальный идентификатор пользователя, тот же, что и в базе с информацией о клиенте (в Cassandra), либо null, если в базе пользователей нет информации об этих посетителях магазина, string
# - event_type – buy или view, соответственно покупка или просмотр товара, string
# - category – категория товаров в магазине, string
# - item_id – идентификатор товара, состоящий из категории и номера товара в категории, string
# - item_price – цена товара, integer
# - timestamp – unix epoch timestamp в миллисекундах

# In[33]:


es_options = {
        "es.nodes": "10.0.0.5:9200",
        "es.batch.write.refresh": "false",
        "es.nodes.wan.only": "true"
}
LogShopDF = spark.read.format("es").options(**es_options).load("visits*")
LogShopDF.printSchema()
LogShopDF.show(1, 200, True)


# In[34]:


visits = spark.read.format("org.elasticsearch.spark.sql").option("es.nodes", "10.0.0.5:9200").load("visits")


# In[35]:


from pyspark.sql.functions import *

visits_cat=visits.filter(col("uid").isNotNull())                 .withColumn("category", lower(col("category")))                 .withColumn('category', regexp_replace('category', '-', '_'))                 .withColumn('category', regexp_replace('category', ' ', '_'))


# In[36]:


visits_cat = visits_cat.select(concat(lit("shop_"), col("category")).alias("category"),                               "event_type","item_id","item_price","timestamp","uid")


# In[37]:


visits_cat.show(5)


# ### Информация о клиентах: cassandra

# - uid – уникальный идентификатор пользователя, string
# - gender – пол пользователя, F или M - string
# - age – возраст пользователя в годах, integer

# In[17]:


spark.conf.set("spark.cassandra.connection.host", "10.0.0.5")
spark.conf.set("spark.cassandra.connection.post", "9042")
spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
spark.conf.set("spark.cassandra.input.consistency.level", "ONE")


# In[18]:


table_opts = {"table": "clients","keyspace": "labdata"}


# In[19]:


clients = spark   .read   .format("org.apache.spark.sql.cassandra")   .options(**table_opts)   .load()


# In[20]:


clients.show(5)


# - age_cat – категория возраста, одна из пяти: 18-24, 25-34, 35-44, 45-54, >=55.

# In[21]:


def age_group(year):
        if (year>=18 and year<= 24):
            return '18-24'
        if (year>= 25 and year<= 34):
            return '25-34'
        if (year>= 35 and year<= 44):
            return '35-44'
        if (year>= 45 and year<= 54):
            return '45-54'    
        if year>= 55:
            return '>=55'


# In[24]:


from pyspark.sql.types import StringType

age_group_udf = udf(age_group, StringType())

clients_url = clients.withColumn('age_cat', age_group_udf('age'))


# In[25]:


clients_url.show(5)


# ### Информация о категориях веб-сайтов: postgre

# - domain (только второго уровня), string
# - category, string

# In[27]:


jdbc_url = "jdbc:postgresql://10.0.0.5:5432/labdata?user=olga_pogodina&password=4BJlHRsc"


# In[28]:


domain_cats = spark     .read     .format("jdbc")     .option("url", jdbc_url)     .option("dbtable", "domain_cats")    .option("driver", "org.postgresql.Driver")    .load()


# In[29]:


domain_cats.printSchema()


# In[30]:


domain_cats = domain_cats.withColumn("category", lower(col("category")))         .withColumn('category', regexp_replace('category', '-', '_'))         .withColumn('category', regexp_replace('category', ' ', '_'))


# In[31]:


domain_cats = domain_cats.select("domain",concat(lit("web_"), col("category")).alias("category"))


# In[32]:


domain_cats.show(5)


# ### Итоговая таблица

# - uid (primary key) – uid пользователя.
# - gender – пол пользователя: M, F.
# - age_cat – категория возраста, одна из пяти: 18-24, 25-34, 35-44, 45-54, >=55.
# - shop_cat, web_cat – категории товаров и категории веб-сайтов.

# In[38]:


df_url.show(5)


# In[222]:


visits_cat.show(5)


# In[223]:


clients_url.show(5)


# In[39]:


clients_url = clients_url.select(col("uid"), col("gender"), col("age_cat"))
df_url = df_url.select(col("uid"), col("furl").alias("url"))
visits_cat = visits_cat.select(col("uid"), col("category").alias("shop_category")) 
domain_cats = domain_cats.select(col("domain").alias("url"), col("category").alias("web_category")) 


# In[43]:


joined_url = df_url.join(domain_cats, 'url', 'left')


# In[44]:


import pyspark.sql.functions as F

joined_url_cnt = joined_url.groupBy(["uid","web_category"])              .agg(F.count("web_category").alias("web_shop"))              .groupBy('uid').pivot('web_category').agg(F.first('web_shop'))


# In[48]:


visits_cat_shop = visits_cat.groupBy(["uid","shop_category"])              .agg(F.count("shop_category").alias("shop_shop"))              .groupBy('uid').pivot('shop_category').agg(F.first('shop_shop'))


# In[49]:


clients = clients_url.join(visits_cat_shop, 'uid', 'left')                     .join(joined_url_cnt, 'uid', 'left')


# In[50]:


clients.count()


# In[51]:


clients.select(col("uid")).distinct().count()


# In[57]:


clients = clients.drop("null")


# In[59]:


clients.write    .format("jdbc")    .option("url", "jdbc:postgresql://10.0.0.5:5432/olga_pogodina")    .option("dbtable", "clients")    .option("user", "olga_pogodina")    .option("password", "4BJlHRsc")    .option("driver", "org.postgresql.Driver")    .save()

