#!/usr/bin/env bash
awk -f points.awk | hadoop fs -put - points.csv
