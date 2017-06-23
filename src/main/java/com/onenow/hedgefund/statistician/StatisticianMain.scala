// spark-shell --master local[4] --packages "org.apache.spark:spark-streaming-kinesis-asl_2.11:2.1.1"
// :load <thisFileName>
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
import com.onenow.hedgefund.lookback.{LookbackFactory, TradingLookback}
import com.onenow.hedgefund.util.Piping
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.joda.time.Seconds

import scala.collection.TraversableOnce

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


// == WINDOW LOOKBACKS ==
import scala.collection.JavaConversions._
val factory = new LookbackFactory()
val tradingLookbacks = factory.getAll


// == INSTANTIATE KINESIS ==
// https://docs.databricks.com/spark/latest/structured-streaming/kinesis.html
val numShards = 1
val serviceType = ServiceType.STATISTICIAN
val appName = ServiceType.REPLAY.toString;
val deployEnv = DeployEnv.STAGING;
val region = "us-east-1" // Kinesis.defaultRegion
val endPoint = "https://kinesis.us-east-1.amazonaws.com"
val streamName = ServiceType.REPLAY.toString + "-" + deployEnv.toString

// == TIMING ==
// streams:
val initialPosition = InitialPositionInStream.TRIM_HORIZON // LATEST, TRIM_HORIZON, AT_TIMESTAMP
// context:
val contextBatchSec = 5
val contextBatchDuration = Seconds(contextBatchSec)
// windowing:
val batchMultiple = 10
val windowLength = Seconds(contextBatchSec*batchMultiple)
val slideInterval = Seconds(contextBatchSec)
// checkpoint:
val streamCheckpointInterval = Milliseconds(contextBatchSec*1000)
val contextRemember = Minutes(1)
val checkpointFolder = "/Users/Shared/"
// timeout:
val streamingContextTimeout = 60L *1000


// == CONFIGURE THE STREAMING CONTEXT ==
// when using spark-submit:
// Spark context available as 'sc' (master = local[4], app id = local-1497911109319).
// Spark session available as 'spark'
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }  // stop the streaming contexts without stopping the spark context
@transient val sc = SparkContext.getOrCreate()
@transient val ssc = new StreamingContext(sc, contextBatchDuration) // use in databricks
@transient val spark = SparkSession.builder().getOrCreate()


// == FUNCTIONS ==
// serializable functions inside objects https://www.nicolaferraro.me/2016/02/22/using-non-serializable-objects-in-apache-spark/
// don't keep sc, ssc, spark in object https://stackoverflow.com/questions/27579474/serialization-exception-on-spark
// DESERIALIZE
// byteArray -> String(byteArray)
case object StatFunctions extends Serializable {
  val getStringFromByteArray = (entry: Array[Byte]) => {
    new String(entry)
  }
  // json -> RecordActivity
  val getDeserializedRecordActivity = (json: String) => {
    val record: RecordActivity = Piping.deserialize(json, classOf[RecordActivity])
    record
  }
  // BOOLEAN
  // RecordActivity -> isPriceDataType(RecordActivity)
  val isPriceDataType = (record:RecordActivity, dataType:DataType) => {
    record.getDatumType.toString.equals(dataType.toString)
  }
  // RecordActivity -> isTiming(RecordActivity)
  val isRealtimeDataTiming = (record: RecordActivity) => {
    record.getDatumTiming.toString.equals(DataTiming.REALTIME.toString)
  }
  // (serieName, value) -> isSeriesName(serieName, checkName)
  val isSeriesNamefunc = (kv: (String, Double), name: String) => {
    kv._1.toString.equals(name)
  }
  // UNBUNDLE
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
  val getValuesFromRecordActivity = (record: RecordActivity, windowSize: List[TradingLookback]) => {
    val value = record.getStoredValue.toDouble //1
    val sum = value //2
    val count = 1.0 //3
    val mean = value / count //4
    val variance = 0.0 //5
    val deviation = 0.0 //6
    val zScore = 0.0 //7
    ((record.getSerieName, windowSize), (value, sum, count, mean, variance, deviation, zScore))
  }
  // SLIDING
  // on a value
  val addNewFunc = (accumulator: Double, current: Double) => {
    accumulator + current
  }
  val subOldFunc = (accumulator: Double, current: Double) => {
    accumulator - current
  }
  // on (value,count)
  val addNewPairFunc = (accumulator: (Double, Double), current: (Double, Double)) => {
    (accumulator._1 + current._1, accumulator._2 + current._2) // values added, counts added
  }
  val subOldPairFunc = (accumulator: (Double, Double), current: (Double, Double)) => {
    (accumulator._1 - current._1, accumulator._2 - current._2) // values subtracted, counts subtracted
  }
  // on several (name, (value, count, mean, variance, deviation))
  val addInsightFunc = (accumulator: (Double, Double, Double, Double, Double, Double, Double),
                        current: (Double, Double, Double, Double, Double, Double, Double)) => {
    val value = current._1
    val valueTotal = accumulator._2 + current._2
    val countTotal = accumulator._3 + current._3
    val meanTodate = valueTotal / countTotal
    val currentScore = value - meanTodate
    val varianceTotal = accumulator._5 + currentScore * currentScore // sum of square scores
    val currentZscore = currentScore / scala.math.sqrt(varianceTotal) // score / standard deviation
    (value, valueTotal, countTotal, meanTodate, currentScore, varianceTotal, currentZscore)
  }
  val subInsightFunc = (accumulator: (Double, Double, Double, Double, Double, Double, Double),
                        current: (Double, Double, Double, Double, Double, Double, Double)) => {
    val value = current._1
    val valueTotal = accumulator._2 - current._2
    val countTotal = accumulator._3 - current._3
    val meanTodate = valueTotal / countTotal
    val currentScore = value - meanTodate
    val varianceTotal = accumulator._5 - currentScore * currentScore // sum of square scores
    val currentZscore = currentScore / scala.math.sqrt(varianceTotal) // score / standard deviation
    (value, valueTotal, countTotal, meanTodate, currentScore, varianceTotal, currentZscore)
  }
// STRUCTURED STREAMING
// https://spark.apache.org/docs/latest/streaming-programming-guide.html
// https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/10%20Window%20Aggregations.html
// http://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=reducebykey
// https://stackoverflow.com/questions/24071560/using-reducebykey-in-apache-spark-scala
// https://stackoverflow.com/questions/28147566/custom-function-inside-reducebykey-in-spark
// sort rdd https://stackoverflow.com/questions/32988536/spark-dstream-sort-and-take-n-elements
//
// NOTE on input dStream: ((name,lookbackWindowSec),(d,d,d,d,d,d,d)), where each d is a statistical measure
  val reduceDstreamByKeyAndWindow = (dStream: DStream[((String,String), (Double,Double,Double,Double,Double,Double,Double))],
                                     windowLengthSec:Long, slideIntervalSec:Long) => {
    (dStream
      .filter(r => r._1._2.equals(windowLengthSec.toString))      // process each item only once (each item was previously flatmapped)
      .reduceByKeyAndWindow(                                      // key not mentioned
      StatFunctions.addInsightFunc,                                             // Adding elements in the new batches entering the window
      StatFunctions.subInsightFunc,                                             // Removing elements from the oldest batches exiting the window
      Seconds(windowLengthSec),                                   // Window duration
      Seconds(slideIntervalSec)                                   // Slide duration
      )
    )
  }
  // PRINT
  val printStats = (x: ((String,String),(Double,Double,Double,Double,Double,Double,Double))) => {
    //  val mean = x._2._1 / x._2._2
    //  val pair = (x._1, mean)  // seriename, mean
    //  if(!pair._2.equals(Double.PositiveInfinity) && !pair._2.equals(Double.NaN)) {
    //    println(pair)
    //  } else {
    //    print(x)
    //  }
    println(x)
  }
  val outputStatsRDD = (x: RDD[(((String,String),(Double,Double,Double,Double,Double,Double,Double)))]) => {
    x.collect().foreach(printStats)
  }
}

// == D-STREAM ==
// In every microbatch get the union of shard streams
// Credentials in the environment, ur use a constructor of KinesisUtils.createStream that take AWS_ACCESS_KEY_ID, AWS_SECRET_KEY
// https://spark.apache.org/docs/latest/streaming-kinesis-integration.html
// https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/streaming/kinesis/KinesisUtils.html
// https://spark.apache.org/docs/latest/streaming-kinesis-integration.html#running-the-example
//
// Create the Kinesis DStreams:
@transient val kinesisDstreams = (0 until numShards).map { i =>
  KinesisUtils.createStream(ssc, appName.toString, streamName, endPoint,
    region.toString, initialPosition, streamCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
}

@transient val unionDstream = ssc.union(kinesisDstreams) // each row is Array[Byte]
// unionDstream.print()


@transient val recordWindowValuesDstream = (unionDstream
    .map(StatFunctions.getStringFromByteArray)                     // string from byte array, getStringFromByteArray
    .map(StatFunctions.getDeserializedRecordActivity)
    .filter(r => StatFunctions.isPriceDataType(r, DataType.PRICE))
    .filter(StatFunctions.isRealtimeDataTiming)
//    .flatMap(record => {
//      val value = record.getStoredValue.toDouble //1
//      val sum = value //2
//      val count = 1.0 //3
//      val mean = value / count //4
//      val variance = 0.0 //5
//      val deviation = 0.0 //6
//      val zScore = 0.0 //7
//      val items = Seq[((String,String),(Double,Double,Double,Double,Double,Double,Double))]()
//      for(lookback <- tradingLookbacks) {
//                val toAdd = ((record.getSerieName, lookback.getWindowSec.toString), (value, sum, count, mean, variance, deviation, zScore))
//                items:+toAdd
//              }
//      items
//      }
//    )

//    .flatMap(r => getValuesFromRecordActivity(r, tradingLookbacks))                // for each stream (pair records for specific windows) turn a RecordActivity into a tuple of statistical values
//    .flatMap(recordValues => {                       // emit a row for every window that will be reduced: ((name,window),(d,d,d,d,d,d,d)
//      val items = Seq[((String,String),(Double,Double,Double,Double,Double,Double,Double))]()
//      for(lookback <- tradingLookbacks) {
//        val toAdd = ((recordValues._1,lookback.getWindowSec.toString),(recordValues._2._1, recordValues._2._1, recordValues._2._1, recordValues._2._1, recordValues._2._1, recordValues._2._1, recordValues._2._1))
//        items:+toAdd
//      }
//      items
//      }
//    )
//  )
  )

recordWindowValuesDstream.print()

// TODO: reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks])
//val kInsightsByLookbackDStreamList = (tradingLookbacks
//    .map(tradingLookbacks => {
//      reduceDstreamByKeyAndWindow(recordWindowValuesDstream, tradingLookbacks.getWindowSec, tradingLookbacks.getSlideIntervalSec)
//    }
//  )


//kInsightsByLookbackDStreamList.map(stream => stream.foreachRDD(outputStatsRDD))





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


// WATERMARKING
// https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

// STREAMING CONFIG
// To make sure data is not deleted by the time we query it interactively
ssc.remember(contextRemember)

ssc.checkpoint(checkpointFolder)

ssc.start()

// This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
ssc.awaitTerminationOrTimeout(streamingContextTimeout) // time to wait in milliseconds; and/or run stopSparkContext above
// ssc.awaitTermination()

// FORCE STOP
// StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
