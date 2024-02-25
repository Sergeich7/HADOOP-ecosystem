#!/usr/bin/env bash

INP_DIR="/data/wiki/en_articles"
#INP_DIR="/data/wiki/en_articles_part"
TMP_DIR="111_res_tmp"
OUT_DIR="111_res_out"

NUM_REDUCERS=8

# Remove previous results
hdfs dfs -rm -r -skipTrash $TMP_DIR*  > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="111_sort_names" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -combiner "python3 reducer.py" \
    -reducer "python3  reducer.py" \
    -input $INP_DIR \
    -output $TMP_DIR  > /dev/null

# Look at the input data
#hdfs dfs -cat ${TMP_DIR}/part-00000 | head -n 10

# Remove previous results
hdfs dfs -rm -r -skipTrash $OUT_DIR*  > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options='-k2,2nr -k1' \
    -files filter.py \
    -mapper "python3 filter.py" \
    -reducer cat \
    -input $TMP_DIR \
    -output $OUT_DIR > /dev/null

# Checking result
hdfs dfs -cat $OUT_DIR/part-00000 | head -n 10
#hdfs dfs -cat ${OUT_DIR}/* | head -10
