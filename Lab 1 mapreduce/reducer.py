#!/usr/bin/env python

import sys
import random

count = 0
max_ids_in_line = random.randint(1,5)
res =''

for line in sys.stdin:
    try:
        rnd, ids = line.strip().split('\t')
        res = res + ids + ","
        count += 1
    except ValueError as e:
        continue

    if count == max_ids_in_line:
        print res[:-1]
#        print >> sys.stderr, "reporter:counter:Custom stats,IDs groups,{}".format(1)
        count = 0
        max_ids_in_line = random.randint(1,5)
        res = ''

if res:
    print res[:-1]
    