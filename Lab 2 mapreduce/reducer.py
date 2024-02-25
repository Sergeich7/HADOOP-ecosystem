#!/usr/bin/env python

import sys

current_key = None
current_count = 0
current_flag = 1

for line in sys.stdin:
    try:
        key, flag, count = line.strip().split('\t', 2)
        count = int(count)
        flag = int(flag)
    except ValueError as e:
        continue

    if current_key != key:
        if current_key:
            print(f"{current_key}\t{current_flag}\t{current_count}")
        current_count = 0
        current_key = key
        current_flag = 1

    current_flag = current_flag & flag
# with if current_flag: CPU time spent (ms)=12562570
# without if: CPU time spent (ms)=12500970
    current_count += count

if current_key:
    print(f"{current_key}\t{current_flag}\t{current_count}")

