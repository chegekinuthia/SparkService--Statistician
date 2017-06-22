#!/usr/bin/env bash

# create folder to contain jars
mkdir configuration
cd configuration

### DEPENDENCIES
# download jars
# curl https://jenkins.hedge.guru/job/Library--Discrete/ws/target/discrete-0.0.0-SNAPSHOT.jar
#
# or copy jars locally
cp /Users/pablo/git/Library--IB-Discrete/target/ibdiscrete-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Discrete/target/discrete-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Util/target/util-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Time/target/time-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Logging/target/logging-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--Event/target/event-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--AWS-Kinesis/target/aws-kinesis-0.0.0-SNAPSHOT.jar .
cp /Users/pablo/git/Library--AWS-Hifawn/target/aws-0.0.0-SNAPSHOT.jar .