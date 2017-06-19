#!/usr/bin/env bash
# https://stackoverflow.com/questions/37132559/add-jars-to-a-spark-job-spark-submit

# Spark master folder
cd ~/Users/pablo/spark-2.1.1-bin-hadoop2.7/

# create folder to contain jars
mkdir conf-jars
cd conf-jars

# download jars
# curl https://jenkins.hedge.guru/job/Library--Discrete/ws/target/discrete-0.0.0-SNAPSHOT.jar

# or copy jars locally
cp /Users/pablo/git/Library--IB-Discrete/target/ibdiscrete-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Discrete/target/discrete-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Util/target/util-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Time/target/time-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Logging/target/logging-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Event/target/event-0.0.0-SNAPSHOT.jar .

# Start spark shell
spark-shell --master local[4] --packages "org.apache.spark:spark-streaming-kinesis-asl_2.11:2.1.1" --conf "spark.driver.extraClassPath=./discrete-0.0.0-SNAPSHOT.jar:./ibdiscrete-0.0.0-SNAPSHOT.jar:./time-0.0.0-SNAPSHOT.jar:./event-0.0.0-SNAPSHOT.jar:./logging-0.0.0-SNAPSHOT.jar:./util-0.0.0-SNAPSHOT.jar"