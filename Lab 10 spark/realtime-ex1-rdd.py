#!/usr/bin/env python3

from pyspark import SparkContext, SparkConf

#config = SparkConf().setAppName("belashov_spark_ex2").setMaster("local[3]")  
config = SparkConf().setAppName("belashov_realtime_ex1").setMaster("yarn")  
sc = SparkContext(conf=config)  

import re
import math
from operator import add

stop_words = sc.textFile("/data/wiki/stop_words_en-xpo6.txt").collect()

wiki_words = sc.textFile("/data/wiki/en_articles_part/") \
    .map(lambda x: x.strip().lower().split('\t', 1)[1]) \
    .flatMap(lambda x: x.split()) \
    .map(lambda x: re.sub(r'^\W+|\W+$', "", x)) \
    .filter(lambda x: x != '') \
    .filter(lambda x: x not in stop_words)

prev_word = ''

def f(x):
    global prev_word

    pw_tmp = prev_word
    prev_word = x

    return ((pw_tmp, x), 1)

wiki_pairs = wiki_words.map(f).filter(lambda x: x[0][0])

total_number_of_words = wiki_words.count()
total_number_of_word_pairs = wiki_pairs.count()

wiki_words = wiki_words.map(lambda x: (x, 1)) \
    .reduceByKey(add)

def calc_npmi(x):
    global total_number_of_words, total_number_of_word_pairs

    p_a = x[2] / float(total_number_of_words)
    p_b = x[3] / float(total_number_of_words)
    p_ab = x[1] / float(total_number_of_word_pairs)
    
    pmi_ab = math.log(p_ab/p_a/p_b)
    npmi_ab = - pmi_ab / math.log(p_ab)

    return (x[0], npmi_ab)

wiki_pairs = wiki_pairs.reduceByKey(add) \
    .filter(lambda x: x[1] >= 500) \
    .map(lambda x: (x[0][0], x)) \
    .join(wiki_words) \
    .map(lambda x: x[1][0] + (x[1][1],)) \
    .map(lambda x: (x[0][1], x)) \
    .join(wiki_words) \
    .map(lambda x: x[1][0] + (x[1][1],)) \
    .map(calc_npmi) \
    .coalesce(1) \
    .sortBy(lambda x: x[1], ascending=False) \
    .map(lambda x: x[0][0] + '_' + x[0][1])

for s in wiki_pairs.take(39):
    print(s)


