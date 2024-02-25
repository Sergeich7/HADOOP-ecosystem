#!/usr/bin/env python

import sys
import random

for line in sys.stdin:
    try:
        line = line.strip()
        rnd = str(random.random())
    except ValueError as e:
        continue
    print "%s\t%s" % (rnd, line)
#    print >> sys.stderr, "reporter:counter:Custom stats,IDs total,{}".format(1)
