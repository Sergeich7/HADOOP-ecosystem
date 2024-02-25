from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import broadcast

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
    .csv("/data/twitter/twitter_sample.txt", sep ='\t', header = False)

start = '12'
finish = '34'

schema = StructType() \
    .add("user_id",StringType(),True) \
    .add("path",StringType(),True)

distancesDF = spark.createDataFrame(data=[(start, start + ",")],schema=schema)

while True:
    distancesDF = graphDF.join(broadcast(distancesDF), "user_id") \
        .select("follower_id", "path") \
        .rdd.map(lambda x: (x.follower_id, x.path + x.follower_id + ',')) \
        .toDF(schema).persist()
    result = distancesDF.filter(distancesDF.user_id == finish).persist()
    if result.count() > 0:
        print(result.take(1)[0][1][0:-1])
        break
    
      
      
      
      
      
      
      
      
      