#!/usr/bin/env bash
hadoop fs -rm -r -skipTrash /user/root/wkt
cat << EOF > /tmp/wkt.properties
# spark.master=yarn-client
# spark.executor.memory=512m
spark.master=local[*]
spark.executor.memory=4g
spark.ui.enabled=false
system.in.read=false
gdb.path=hdfs:///user/root/TXDOT_Roadway_Inventory.gdb
point.path=hdfs:///user/root/points.csv
output.path=hdfs:///user/root/wkt
output.format=wkt
EOF
time spark-submit\
 --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0\
 target/spark-snap-points-0.3.jar\
 /tmp/wkt.properties
