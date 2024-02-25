#!/usr/bin/env bash

PYSPARK_DRIVER_PYTHON=/usr/bin/python3
PYSPARK_PYTHON=/usr/bin/python3

spark-submit realtime-ex2-hll.py | grep -v "WARNING"

