#!/usr/bin/env bash

OUT_DIR="streaming_ids_result"$(date +"%s%6N")
NUM_REDUCERS=5

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="belashov-homework-01-hadoop" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -input /data/ids \
    -output $OUT_DIR > /dev/null

hdfs dfs -cat ${OUT_DIR}/* | head -50

