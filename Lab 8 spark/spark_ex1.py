#!/usr/bin/env python3

from pyspark import SparkContext, SparkConf
import re

config = SparkConf().setAppName("belashov_spark_ex1").setMaster("yarn")
spark_context = SparkContext(conf=config)

#rdd = spark_context.textFile("/data/wiki/en_articles") \
rdd = spark_context.textFile("/data/wiki/en_articles_part") \
.map(lambda x: x.strip().lower()) \
.flatMap(lambda x: re.findall(r'narodnaya [a-z]+', x)) \
.map(lambda x: (x.replace(' ', '_'), 1)) \
.reduceByKey(lambda a, b: a + b) \
.sortBy(lambda a: a[0], ascending=True)

bigrams_count = rdd.take(10)

for bigram, count in bigrams_count:
    print(bigram.encode("utf8") + " " + str(count))
