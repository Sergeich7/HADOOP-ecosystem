#!/usr/bin/env python

import sys

for line in sys.stdin:
    try:
        key, flag, count = line.strip().split('\t', 2)
    except ValueError as e:
        continue
        
    if flag == '1':
        print(f"{key}\t{count}")
