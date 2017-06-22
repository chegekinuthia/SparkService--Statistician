// spark-shell --master local[4] --packages "org.apache.spark:spark-streaming-kinesis-asl_2.11:2.1.1"
// :load /Users/pablo/git/SparkService--Samplean/src/main/java/com/onenow/hedgefund/sparksamplean/SampleanMain.scala
//
// dependency: spark-2.1.1-bin-hadoop2.7
//
// also needed:
// val AWS_ACCESS_KEY_ID =
// val AWS_SECRET_KEY =
//
// monitoring: http://localhost:4040/jobs/
//
// example https://github.com/snowplow/spark-streaming-example-project

// IMPORT THIRD PARTY
import java.util

import com.amazonaws.regions._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.onenow.hedgefund.discrete.{DataTiming, DataType, DeployEnv, ServiceType}
import com.onenow.hedgefund.event.RecordActivity
import com.onenow.hedgefund.util.Piping
import org.apache.spark
import org.apache.spark.rdd.RDD

// import org.apache.spark.storage.StorageLevel
// import org.apache.spark.streaming.kinesis.KinesisUtils
// import org.apache.spark.streaming.{Duration, Milliseconds, Seconds, StreamingContext}

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis._
import org.apache.spark.streaming.{StreamingContext, Seconds, Minutes, Time, Duration}
import org.apache.spark.streaming.dstream._

import java.nio.ByteBuffer
import scala.util.Random


// INSTANTIATE KINESIS
// https://docs.databricks.com/spark/latest/structured-streaming/kinesis.html
val numShards = 1
val serviceType = ServiceType.STATISTICIAN
val appName = ServiceType.REPLAY.toString;
val deployEnv = DeployEnv.STAGING;
val region = "us-east-1" // Kinesis.defaultRegion
val endPoint = "https://kinesis.us-east-1.amazonaws.com"
val streamName = ServiceType.REPLAY.toString + "-" + deployEnv.toString
val initialPosition = InitialPositionInStream.TRIM_HORIZON // LATEST, TRIM_HORIZON, AT_TIMESTAMP

// TIMING
// context
val contextBatchSec = 5
val contextBatchDuration = Seconds(contextBatchSec)
// windowing
val batchMultiple = 10
val windowLength = Seconds(contextBatchSec*batchMultiple)
val slideInterval = Seconds(contextBatchSec)


// CONFIGURE THE STREAMING CONTEXT
// when using spark-submit:
// Spark context available as 'sc' (master = local[4], app id = local-1497911109319).
// Spark session available as 'spark'
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }  // stop the streaming contexts without stopping the spark context
val sc = SparkContext.getOrCreate()
val ssc = new StreamingContext(sc, contextBatchDuration) // use in databricks
val spark = SparkSession.builder().getOrCreate()

// D-STREAM:
// In every microbatch get the union of shard streams
// Credentials in the environment, ur use a constructor of KinesisUtils.createStream that take AWS_ACCESS_KEY_ID, AWS_SECRET_KEY
// https://spark.apache.org/docs/latest/streaming-kinesis-integration.html
// https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/streaming/kinesis/KinesisUtils.html
// https://spark.apache.org/docs/latest/streaming-kinesis-integration.html#running-the-example
//
// Create the Kinesis DStreams:
val kinesisDstreams = (0 until numShards).map { i =>
  KinesisUtils.createStream(ssc, appName.toString, streamName, endPoint,
    region.toString, initialPosition, Milliseconds(contextBatchSec*1000), StorageLevel.MEMORY_AND_DISK_2)
}

val unionDstream = ssc.union(kinesisDstreams) // each row is Array[Byte]
// unionDstream.print()

val jsonDstream = unionDstream.map(byteArray => new String(byteArray))
// jsonDstream.print()

// DESERIALIZE AND FILTER
// transform: json -> RecordActivity
val deserializefunc = (json: String) => {
  val record:RecordActivity = Piping.deserialize(json, classOf[RecordActivity])
  record
}
// RecordActivity -> isDataType(RecordActivity)
val isPricefunc = (record: RecordActivity) => {
  record.getDatumType.toString.equals(DataType.PRICE.toString)
}
// RecordActivity -> isTiming(RecordActivity)
val isRealtimefunc = (record: RecordActivity) => {
  record.getDatumTiming.toString.equals(DataTiming.REALTIME.toString)
}
// RecordActivity -> (serieName, value)
val getKVfunc = (record: RecordActivity) => {
  (record.getSerieName, record.getStoredValue.toDouble)
}
// RecordActivity -> (serieName, 1)
val getKIncfunc = (record: RecordActivity) => {
  (record.getSerieName, 1.0)
}
// RecordActivity -> (serieName, (value,1))
val getKValueIncfunc = (record: RecordActivity) => {
  (record.getSerieName, (record.getStoredValue.toDouble, 1.0))
}
// RecordActivity -> (serieName, (value,1,mean,variance,deviation))
val getValuesfunc = (record: RecordActivity) => {
  (record.getSerieName, (record.getStoredValue.toDouble, 1.0, 0.0, 0.0, 0.0, 0.0))
}
// (serieName, value) -> isSeriesName(serieName, checkName)
val isSeriesNamefunc = (kv:(String,Double), name:String) => {
  kv._1.toString.equals(name)
}

val recordsDstream = jsonDstream.map(deserializefunc).filter(isPricefunc) // .filter(isRealtimefunc)
//recordsDstream.print()

val kValueIncDstream = recordsDstream.map(getKValueIncfunc)
//kValueIncDstream.print()

val kValuesDstream = recordsDstream.map(getValuesfunc)
//kValuesDstream.print()


// CALCULATE THE MEAN: STRUCTURED STREAMING
// calculate the average
// https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/10%20Window%20Aggregations.html
// http://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=reducebykey
// https://stackoverflow.com/questions/24071560/using-reducebykey-in-apache-spark-scala
// https://stackoverflow.com/questions/28147566/custom-function-inside-reducebykey-in-spark
// sort rdd https://stackoverflow.com/questions/32988536/spark-dstream-sort-and-take-n-elements

// SLIDING FUNCTIONS
// on a value
val addNewFunc = (x: Double, y:Double) => {
  x+y
}
val subOldFunc = (x: Double, y:Double) => {
  x-y
}
// on (value,count)
val addNewPairFunc = (x: (Double,Double), y:(Double,Double)) => {
  (x._1+y._1, x._2+y._2)  // values added, counts added
}
val subOldPairFunc = (x: (Double,Double), y:(Double,Double)) => {
  (x._1-y._1, x._2-y._2)  // values subtracted, counts subtracted
}
// on several (name, (value, count, mean, variance, deviation))
val addInsightFunc = (accumulator: (Double,Double,Double,Double,Double,Double), current:(Double,Double,Double,Double,Double,Double)) => {

  val valueTotal = accumulator._1 + current._1
  val countTotal = accumulator._2 + current._2
  val meanTodate = valueTotal/countTotal
  val currentScore = current._1 - meanTodate
  val varianceTotal = accumulator._4 + currentScore * currentScore  // sum of square scores
  val currentZscore = currentScore / scala.math.sqrt(varianceTotal) // score / standard deviation

  (valueTotal, countTotal, meanTodate, currentScore, varianceTotal, currentZscore)
}
val subInsightFunc = (accumulator: (Double,Double,Double,Double,Double,Double), current:(Double,Double,Double,Double,Double,Double)) => {

  val valueTotal = accumulator._1 - current._1
  val countTotal = accumulator._2 - current._2
  val meanTodate = valueTotal/countTotal
  val currentScore = current._1 - meanTodate
  val varianceTotal = accumulator._4 - currentScore * currentScore  // sum of square scores
  val currentZscore = currentScore / scala.math.sqrt(varianceTotal) // score / standard deviation

  (valueTotal, countTotal, meanTodate, currentScore, varianceTotal, currentZscore)
}

import collection.mutable.HashMap
val lastMean = new HashMap[String, Double]()


// REDUCE
// config:
// https://spark.apache.org/docs/latest/streaming-programming-guide.html
// TODO: reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks])

val kInsightsDStream = kValuesDstream.reduceByKeyAndWindow(  // key not mentioned
  addInsightFunc,       // Adding elements in the new batches entering the window
  subInsightFunc,       // Removing elements from the oldest batches exiting the window
  windowLength,        // Window duration
  slideInterval)     // Slide duration
//kInsightsDStream.print()

// PRINT
val printMean = (x: (String, (Double,Double,Double,Double,Double,Double))) => {
  //  val mean = x._2._1 / x._2._2
  //  val pair = (x._1, mean)  // seriename, mean
  //  if(!pair._2.equals(Double.PositiveInfinity) && !pair._2.equals(Double.NaN)) {
  //    println(pair)
  //  } else {
  //    print(x)
  //  }
  println(x)
}

val printPairRDD = (x: RDD[(String, (Double,Double,Double,Double,Double,Double))]) => {
  x.collect().foreach(printMean)
}

kInsightsDStream.foreachRDD(printPairRDD)


// JOIN
// https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/13%20Joining%20DStreams.html
// When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W))
// TODO: join(otherDataset, [numTasks])
//val meanByKey = kvSumDStream.join(kvCountrDStream).map(joined => {
//              val sum = joined._2._1
//              val count = joined._2._2
//              val mean = sum/count
//              (joined._1, mean, sum, count)  // serieName, mean...
//            }
//)
//meanByKey.print()

// Register a temp table at every batch interval so that it can be queried separately
// https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/13%20Joining%20DStreams.html
//meanByKey.window(windowLength).foreachRDD { rdd =>  // Duration(60000)
//  // spark.sqlContext.createDataFrame(rdd).toDF("adId", "clicks", "impressions", "CTR", "Time").registerTempTable("ctr")
//  rdd.take(1)
//}


// CALCULATE THE MEAN: WINDOWED STREAMING
//val kValuesWindowDstream10 = kvDstream.window(Seconds(contextBatchSec*10))

//val getMeanFunc = (kv:(String,Int)) => {
//
//}

// reduceByKey
//(SPY-STOCK-TRADED,ArrayBuffer(244.05, 244.05, 244.05, 244.05, 244.05, 244.04, 244.04, 244.04, 244.05, 244.05, 244.05, 244.06, 244.06, 244.06, 244.06, 244.05, 244.05, 244.05, 244.06, 244.06, 244.06, 244.06, 244.06, 244.05, 244.05, 244.05, 244.04, 244.04, 244.05, 244.05, 244.05, 244.04, 244.05, 244.05, 244.05, 244.04, 244.06, 244.06, 244.06, 244.06, 244.06, 244.06, 244.06, 244.07, 244.06, 244.06, 244.06, 244.06, 244.05, 244.05, 244.05, 244.06, 244.06, 244.06, 244.06, 244.06, 244.06, 244.06, 244.05, 244.05, 244.04, 244.04, 244.03, 244.03, 244.03, 244.03, 244.03, 244.03, 244.03, 244.04, 244.04, 244.03, 244.04, 244.04, 244.04, 244.03, 244.03, 244.06, 244.03, 244.05, 244.04, 244.03, 244.04, 244.03, 244.04, 244.03, 244.04, 244.04, 244.04, 244.04, 244.03, 244.03, 244.03, 244.03, 244.03, 244.04, 244.02, 244.01, 244.01, 244.01, 244.02, 244.03, 244.03, 244.02, 244.02, 244.02, 244.01, 244.01, 244.01, 244.02, 244.02, 244.01, 244.01, 244.02, 244.01, 244.01, 244.02, 244.02, 244.01, 244.02, 244.01, 244.01, 244.06, 244.01, 244.01, 244.02, 244.02, 244.03))
//val kValuesDstream = kValuesWindowDstream10.groupByKey()

//val kSumDstream = kvDstream.reduceByKey(_+_)  // add up values
// kSumDstream.print()

//val kSizeDstream = kValuesDstream.map(kv => (kv._1, kv._2.size))  // count values
// kSizeDstream.print()



// OUTPUT
// joins https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/13%20Joining%20DStreams.html
//val una = kvSumDStream.filter(r => isSeriesNamefunc(r, "SPY-STOCK-TRADED"))
//una.foreachRDD()

// WATERMARKING
// https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

// STREAMING CONFIG
// To make sure data is not deleted by the time we query it interactively
ssc.remember(Minutes(1))

ssc.checkpoint("/Users/Shared/")

ssc.start()

// This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
ssc.awaitTerminationOrTimeout(60L *1000) // time to wait in milliseconds; and/or run stopSparkContext above
// ssc.awaitTermination()

// FORCE STOP
// StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

