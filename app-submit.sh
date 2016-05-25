#!/usr/bin/env bash
hadoop fs -rm -r -skipTrash /user/root/wkt
spark-submit\
 --packages com.databricks:spark-avro_2.10:2.0.1,com.databricks:spark-csv_2.10:1.4.0\
 target/spark-snap-points-0.1.jar\
 app.properties
