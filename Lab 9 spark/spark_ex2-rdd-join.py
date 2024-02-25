#!/usr/bin/env python3

from pyspark import SparkContext, SparkConf

def parse_edge(s):
  user, follower = s.split("\t")
  return (follower, user)

def step(item):
  prev_v, prev_path, next_v = item[0], item[1][0], item[1][1]
  return (next_v, prev_path + next_v + ',')

#config = SparkConf().setAppName("belashov_spark_ex2").setMaster("local[3]")  
config = SparkConf().setAppName("belashov_spark_ex2").setMaster("yarn")  
sc = SparkContext(conf=config)  

n = 400
forward_edges = sc.textFile("/data/twitter/twitter_sample.txt").map(parse_edge).partitionBy(n)

start = '12'
finish = '34'

distances = sc.parallelize([(start, start + ',')]).partitionBy(n)
while True:
    candidates = distances.join(forward_edges, n).map(step).persist()
    result = candidates.filter(lambda i: i[0] == finish).persist()
    if result.count() > 0:
        print(result.take(1)[0][1][0:-1])
        break
    distances = candidates 


        
 