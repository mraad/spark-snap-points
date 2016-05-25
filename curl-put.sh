#!/usr/bin/env bash
pushd /tmp
curl -O http://ftp.dot.state.tx.us/pub/txdot-info/tpp/roadway-inventory/2014.zip
unzip 2014.zip TXDOT_Roadway_Inventory.gdb/*
hadoop fs -put TXDOT_Roadway_Inventory.gdb
popd
awk -f points.awk | hadoop fs -put - points.csv
