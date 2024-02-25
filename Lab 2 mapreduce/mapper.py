#!/usr/bin/env python

import sys
import re

for line in sys.stdin:
    try:
        article_id, text = line.strip().split('\t', 1)
    except ValueError as e:
        continue

    words = re.findall(r'\w+', text)

    for word in words:
        if re.fullmatch(r'[A-Z][a-z]{5,8}', word):
            print(f"{word.lower()}\t1\t1")
        elif re.fullmatch(r'[a-z]{6,9}', word):
            print(f"{word.lower()}\t0\t1")

