# SparkService--Statistician

This service computes streaming values: mean, variance, score, zScore 

It does so for any number of input time series

And across any number of time windows


### Run on Spark shell

#### On Spark shell

cd resources

bash gatherJars.sh 

bash startSpark.sh 

:load StatisticianMain.scala 


#### Submitting to a Spark cluster

Follow the instructions in https://spark.apache.org/docs/latest/streaming-kinesis-integration.html#running-the-example


### Dependency

#### Spark

spark-2.1.1-bin-hadoop2.7

#### 3rd parth jars

See startSpark.sh in the resources folder



### Monitoring

http://localhost:4040/jobs/