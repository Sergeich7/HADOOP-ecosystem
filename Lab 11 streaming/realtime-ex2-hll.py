import time
import os
import subprocess

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

from hdfs import Config
from ua_parser import user_agent_parser
from hyperloglog import HyperLogLog

client = Config().get_client()
nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")

sc = SparkContext(master='yarn-client')

# Preparing base RDD with the input data
DATA_PATH = "/data/realtime/uids"

batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)[:30]]

# Creating QueueStream to emulate realtime data generating
BATCH_TIMEOUT = 2 # Timeout between batch generation
ssc = StreamingContext(sc, BATCH_TIMEOUT)
dstream = ssc.queueStream(rdds=batches)

printed = False

EMPTY_BATCHES_THRESHOLD = 1
finished = 0

def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished += 1
    else:
        finished = 0

def print_only_at_the_end(rdd):
    global finished, printed
    rdd.first()

    if not printed and finished >= EMPTY_BATCHES_THRESHOLD:
        for st in rdd.collect():
            print("{}\t{}".format(*st).lower())
        printed = True


dstream.foreachRDD(set_ending_flag)


def get_segments(x):

    (uid, ua) = x.strip().split('\t')
    parsed_ua = user_agent_parser.Parse(ua)
    
    result = []

    if parsed_ua['device']['family'] == 'iPhone':
        result.append(('seg_iphone', uid))
    if parsed_ua['user_agent']['family'] == 'Firefox':
        result.append(('seg_firefox', uid))
    if parsed_ua['os']['family'] == 'Windows':
        result.append(('seg_windows', uid))

    return result


def update_uids(new_uids, hll):
    hll = hll or HyperLogLog(0.01)

    for uid in new_uids:
        hll.add(uid)

    return hll

dstream.flatMap(get_segments) \
       .updateStateByKey(update_uids) \
       .map(lambda seg_hll: (seg_hll[0], len(seg_hll[1]))) \
       .foreachRDD(print_only_at_the_end)

ssc.checkpoint('./checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))  # checkpoint for storing current state
ssc.start()
while not printed:
    time.sleep(0.1)
ssc.stop()


