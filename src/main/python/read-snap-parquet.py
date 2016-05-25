#!/usr/bin/env python
# encoding: utf-8

import json
import parquet
import glob

for filename in glob.glob("/tmp/tmp/*.parquet"):
    with open(filename) as fo:
        for row in parquet.DictReader(fo, columns=['px', 'py', 'x', 'y', 'distanceToLine']):
            print(json.dumps(row))
