#!/usr/bin/env bash
hadoop fs -rm -r -skipTrash /user/root/avro
cat << EOF > /tmp/avro.properties
spark.ui.enabled=true
system.in.read=true
gdb.path=hdfs:///user/root/TXDOT_Roadway_Inventory.gdb
point.path=hdfs:///user/root/points.csv
output.path=hdfs:///user/root/avro
output.format=avro
EOF
time spark-submit\
 --master yarn-client\
 --num-executors 6\
 --executor-cores 1\
 --executor-memory 512M\
 --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0\
 target/spark-snap-points-0.3.jar\
 /tmp/avro.properties
