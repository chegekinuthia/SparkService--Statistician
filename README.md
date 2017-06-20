# SparkService--Samplea

### Run on Spark shell

#### On Spark shell

bash resources/gatherJars.sh 

bash resources/startSpark.sh 

:load job.scala 

#### Submitting to a Spark cluster

Follow the instructions in https://spark.apache.org/docs/latest/streaming-kinesis-integration.html#running-the-example


### Dependency

#### Spark

spark-2.1.1-bin-hadoop2.7

#### 3rd parth jars

See startSpark.sh in the resources folder



### Monitoring

http://localhost:4040/jobs/