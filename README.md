# SparkService--Statistician

This service computes streaming summary statistics: e.g. mean, deviation, zScore, variance, standard deviation.

It does so for any number of input time series.

And across any number of time windows.

Then, it joins the output d-streams to also emit: covariance, correlation.
 

## Sample Input

{"datumSource": "IB", "datumType": "SIZE", "datumTiming": "STREAMING", "serieName": "SPY-STOCK-TRADED-DIA-STOCK-TRADED", "storedValue": "39.0", "sectorName": "DOWJONES", "timeInMsec": 1496674824867} 

{"datumSource": "IB", "datumType": "SIZE", "datumTiming": "STREAMING", "serieName": "SPY-STOCK-TRADED-SPXU-STOCK-TRADED", "storedValue": "285.0", "sectorName": "DOWJONES","timeInMsec": 1496674824877} 

#### Where
datumSource is the provider of the data, datumType is the type of fata, datumTiming is the timing nature of the data, serieName is the name of the series, storedValue, sectorName is the value, timeInMsec is the time stamp.

## Sample Output 

### Streaming Stats

((SPY-STOCK-TRADED-SPY-STOCK-ASK,DOWJONES,600),(2683.45,268360.2599999996,100.0,2683.602599999996,2.7028083560741143,1496676733178))

((SPY-STOCK-TRADED-SPY-STOCK-BID,DOWJONES,600),(2683.35,268350.3600000004,100.0,2683.503600000004,2.7470825191227606,1496676733178))

#### Where
((serieName, sectorName, windowSizeSeconds, windowSlideSec), (lastValue, valueTotal, countTotal, meanTodate, sumOfSquaredDeviations, timeInMsec)

### Streaming Co-Stats

((SPY-STOCK-TRADED-SPY-STOCK-MARK_PRICE,SPY-STOCK-TRADED-SPY-STOCK-BID,300,25),(0.003401936038715627,0.05832611798084652,1496676066587))

((SPY-STOCK-TRADED-SPY-STOCK-MARK_PRICE,SPY-STOCK-TRADED-SPY-STOCK-MARK_PRICE,300,25),(0.003335905416254516,0.057757297515158346,1496676066587))

#### Where

((serie1Name, serie2Name, windowSizeSeconds, windowSlideSec), (coVariance1to2, coRrelation1to2, timeInMSec1to2)


### Run on Spark Shell

#### On Spark Shell

cd resources

bash gatherJars.sh 

bash startSpark.sh 

:load StatisticianMain.scala 


#### Submitting to a Spark cluster

Follow the instructions in https://spark.apache.org/docs/latest/streaming-kinesis-integration.html#running-the-example


### Dependencies

#### Spark

spark-2.1.1-bin-hadoop2.7

#### 3rd parth jars

See startSpark.sh in the resources folder



### Monitoring

http://localhost:4040/jobs/