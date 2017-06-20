#!/usr/bin/env bash
# https://stackoverflow.com/questions/37132559/add-jars-to-a-spark-job-spark-submit

### MAIN
cp /Users/pablo/git/SparkService--Samplean/src/main/java/com/onenow/hedgefund/sparksamplean/SampleanMain.scala ./configuration/job.scala

cd configuration

# Start spark shell
spark-shell --master local[4] --packages "org.apache.spark:spark-streaming-kinesis-asl_2.11:2.1.1" --conf "spark.driver.extraClassPath=./discrete-0.0.0-SNAPSHOT.jar:./ibdiscrete-0.0.0-SNAPSHOT.jar:./time-0.0.0-SNAPSHOT.jar:./event-0.0.0-SNAPSHOT.jar:./logging-0.0.0-SNAPSHOT.jar:./util-0.0.0-SNAPSHOT.jar"