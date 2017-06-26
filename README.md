# SparkService--Statistician

This service computes streaming summary statistics: mean, deviation, zScore, variance, standard deviation.

It does so for any number of input time series.

And across any number of time windows.

Then, it joins the output d-streams to also emit: covariance, correlation.
 

## Sample Input

#### {"datumSource": "IB", "datumType": "SIZE", "datumTiming": "STREAMING", "serieName": "SPY-STOCK-TRADED-DIA-STOCK-TRADED", "storedValue": "39.0", "sectorName": "DOWJONES", "timeInMsec": 1496674824867} 
#### {"datumSource": "IB", "datumType": "SIZE", "datumTiming": "STREAMING", "serieName": "SPY-STOCK-TRADED-SPXU-STOCK-TRADED", "storedValue": "285.0", "sectorName": "DOWJONES","timeInMsec": 1496674824877} 

#### Where
datumSource is the provider of the data, datumType is the type of fata, datumTiming is the timing nature of the data, serieName is the name of the series, storedValue, sectorName is the value, timeInMsec is the time stamp.

## Sample Output

### Streaming stats

#### ((SPY-STOCK-TRADED-DIA-STOCK-TRADED,DOWJONES,3600),(2683.0100399999997,6449830.09722,2403.0,2684.074114531835,-1.0640745318355584,0.3801410763216977,-1.7258364957615677))
#### ((SPY-STOCK-TRADED-SPXU-STOCK-TRADED,DOWJONES,86400),(2683.1099999999997,6449944.32,2403.0,2684.121647940075,-1.0116479400753633,0.22321927262659114,-2.1412311721498805))
#### ((SPY-STOCK-TRADED-DIA-STOCK-TRADED,DOWJONES,3600),(2683.0099999999998,6449706.320000002,2403.0,2684.022605076988,-1.0126050769881658,0.22371627469373934,-2.1408750016946074))

#### Where
((serieName, sectorName, windowSizeSeconds), (lastValue, valueTotal, countTotal, meanTodate, currentScore, varianceTotal, currentZscore)


### Run on Spark shell

#### On Spark shell

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