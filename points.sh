#!/usr/bin/env bash
hadoop fs -rm -skipTrash points.csv
awk -f points.awk | hadoop fs -put - points.csv
