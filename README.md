# BigDataMining

## Master Command
spark-class org.apache.spark.deploy.master.Master

## Worker Command
spark-class org.apache.spark.deploy.worker.Worker spark://192.168.40.130:7077

## Execute Command
spark-submit --master spark://192.168.40.130:7077 main.py

## Hadoop
start-dfs.sh && start-yarn.sh
stop-dfs.sh && stop-yarn.sh

## HDFS Dir
hdfs dfs -ls /

## Create HDFS Dir
hdfs dfs -mkdir /user/sparkmaster/input/

## Put HDFS
hdfs dfs -put file /user/sparkmaster/input/

## Get HDFS
hdfs dfs -get /user/sparkmaster/output /home/sparkmaster/Downloads/