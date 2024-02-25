from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .master("local") \
    .appName("BelashovSparkEx2") \
    .getOrCreate()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)  
         
schema = StructType() \
    .add("follower_id",StringType(),True) \
    .add("user_id",StringType(),True)

graphDF = spark.read \
    .schema(schema) \
    .csv("/data/twitter/twitter_sample.txt", sep ='\t', header = False) \
    .createOrReplaceTempView('graphV')

start = '12'
finish = '34'

schema = StructType() \
    .add("user_id",StringType(),True) \
    .add("path",StringType(),True)

distancesDF = spark.createDataFrame(data=[(start, start + ",")],schema=schema)

while True:
    distancesDF.createOrReplaceTempView('distancesV')
    distancesDF = spark.sql(''' 
select
    /*+ BROADCAST(d)*/
    g.follower_id as user_id, concat(d.path, g.follower_id, ",") as path 
from graphV g 
join distancesV d on g.user_id == d.user_id
    ''').persist()
    result = distancesDF.filter(distancesDF.user_id == finish).persist()
    if result.count() > 0:
        print(result.take(1)[0][1][0:-1])
        break
    
      
      
      
      
      
      
      
      
      