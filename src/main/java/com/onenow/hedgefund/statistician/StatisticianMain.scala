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


// == IMPORT ==
// NOW
import com.onenow.hedgefund.discrete.{DataTiming, DataType, DeployEnv, ServiceType}
import com.onenow.hedgefund.event.PairActivity
import com.onenow.hedgefund.util.Piping
import com.onenow.hedgefund.integration.SectorName
// AWS
import com.amazonaws.regions._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
// SPARK
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis._
import org.apache.spark.streaming.{StreamingContext, Seconds, Minutes, Time, Duration}
import org.apache.spark.streaming.dstream._
import org.apache.spark
import org.apache.spark.rdd.RDD
// import org.apache.spark.storage.StorageLevel
// import org.apache.spark.streaming.kinesis.KinesisUtils
// import org.apache.spark.streaming.{Duration, Milliseconds, Seconds, StreamingContext}
// OTHER
import java.util
import java.nio.ByteBuffer
import scala.util.Random


// == WINDOW LOOKBACKS ==
import scala.collection.JavaConversions._
import collection.mutable._
import com.onenow.hedgefund.lookback.{LookbackFactory, TradingLookback}
val factory = new LookbackFactory()
val lookbacks = factory.getFast


// == INSTANTIATE KINESIS ==
// https://docs.databricks.com/spark/latest/structured-streaming/kinesis.html
val numShards = 1
val serviceType = ServiceType.STATISTICIAN
val appName = ServiceType.REPLAY.toString;
val deployEnv = DeployEnv.STAGING;
val region = "us-east-1" // Kinesis.defaultRegion
val endPoint = "https://kinesis.us-east-1.amazonaws.com"
val inputStreamName = ServiceType.REPLAY.toString + "-" + deployEnv.toString


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
@transient
val sc = SparkContext.getOrCreate()
@transient
val ssc = new StreamingContext(sc, contextBatchDuration) // use in databricks
@transient
val spark = SparkSession.builder().getOrCreate()


// == FUNCTIONS ==
// serializable functions inside objects https://www.nicolaferraro.me/2016/02/22/using-non-serializable-objects-in-apache-spark/
// don't keep sc, ssc, spark in object https://stackoverflow.com/questions/27579474/serialization-exception-on-spark
// DESERIALIZE
// byteArray -> String(byteArray)
case object StatFunctions extends Serializable {
  val getStringFromByteArray = (entry: Array[Byte]) => {
    new String(entry)
  }
  // json -> PairActivity
  val getDeserializedPairActivity = (json: String) => {
    val event: PairActivity = Piping.deserialize(json, classOf[PairActivity])
    event
  }
  // BOOLEAN
  // PairActivity -> isDataType(PairActivity)
  val isDataType = (event:PairActivity, dataType:DataType) => {
    event.getDatumType.toString.equals(dataType.toString)
  }
  // PairActivity -> isTiming(PairActivity)
  val isDataTiming = (event: PairActivity, dataTiming:DataTiming) => {
    event.getDatumTiming.toString.equals(dataTiming.toString)
  }
  // (serieName, value) -> isSeriesName(serieName, checkName)
  val isSeriesNamefunc = (kv: (String, Double), name: String) => {
    kv._1.toString.equals(name)
  }
  // UNBUNDLE
  // PairActivity -> ((serieName,sectorName,lookback) (d,d,d,d,d,d,d)) for every lookback window
  val getWindowValuesFromPairActivity = (event:PairActivity, lookbacks:List[TradingLookback]) => {
    val value = event.getStoredValue.toDouble  //1
    val sum = value                             //2
    val count = 1.0                             //3
    val mean = value / count                    //4
    val variance = 0.0                          //5
    val deviation = 0.0                         //6
    val zScore = 0.0                            //7

    import scala.collection.mutable.ListBuffer
    val items = ListBuffer[((String,String,String),(Double,Double,Double,Double,Double,Double,Double))]()

    for(lookback <- lookbacks) {  // expand to one record per window
      val toAdd = ((event.getSerieName, event.getSectorName.toString, lookback.getWindowSec.toString),
        (value, sum, count, mean, variance, deviation, zScore))
      items += toAdd
    }
    items.toList
  }
  // STRUCTURED STREAMING
  // https://spark.apache.org/docs/latest/streaming-programming-guide.html
  // https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/10%20Window%20Aggregations.html
  // http://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=reducebykey
  // https://stackoverflow.com/questions/24071560/using-reducebykey-in-apache-spark-scala
  // https://stackoverflow.com/questions/28147566/custom-function-inside-reducebykey-in-spark
  // sort rdd https://stackoverflow.com/questions/32988536/spark-dstream-sort-and-take-n-elements
  //
  // on several (name, (value, count, mean, variance, deviation))
  val addEventToStats = (accumulator: (Double, Double, Double, Double, Double, Double, Double),
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
  val subtractEventsFromStats = (accumulator: (Double, Double, Double, Double, Double, Double, Double),
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
  // EMIT OUTPUT
  val emitStats = (entry: ((String,String,String),(Double,Double,Double,Double,Double,Double,Double))) => {
    println(entry)
  }
  val emitRddStats = (rdd: RDD[((String,String,String),(Double,Double,Double,Double,Double,Double,Double))]) => {
    rdd.collect().foreach(emitStats)
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
@transient
val kinesisDstreams = (0 until numShards).map { i =>
  KinesisUtils.createStream(ssc, appName.toString, inputStreamName, endPoint,
    region.toString, initialPosition, streamCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
}

@transient
val unionDstream = ssc.union(kinesisDstreams) // each row is Array[Byte]
// unionDstream.print()

@transient
val eventValuesAllWindowsDstream = (unionDstream
    .map(StatFunctions.getStringFromByteArray)                     // string from byte array, getStringFromByteArray
    .map(StatFunctions.getDeserializedPairActivity)
    .filter(r => StatFunctions.isDataType(r, DataType.PRICE))
    .filter(r => StatFunctions.isDataTiming(r, DataTiming.REALTIME))
    .flatMap(r => StatFunctions.getWindowValuesFromPairActivity(r, lookbacks.toList))
  )
eventValuesAllWindowsDstream.print()


val windowStatsDstreamList = (lookbacks.toList.map(lookback => {
    eventValuesAllWindowsDstream
      .filter(r => r._1._2.equals(lookback.getWindowSec.toString))  // for each lookback process only items flatmapped for that
      .reduceByKeyAndWindow(                                        // key not mentioned
        StatFunctions.addEventToStats,                              // Adding elements in the new batches entering the window
        StatFunctions.subtractEventsFromStats,                      // Removing elements from the oldest batches exiting the window
        Seconds(lookback.getWindowSec),                             // Window duration
        Seconds(lookback.getSlideIntervalSec)                       // Slide duration
      )
    }
  )
)

// == OUTPUT ==
windowStatsDstreamList.map(stream => stream.foreachRDD(StatFunctions.emitRddStats))


// == WATERMARKING ==
// TODO: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html


// == STREAMING CONFIG ==

ssc.remember(contextRemember)     // Make sure data is not deleted by the time we query it interactively

ssc.checkpoint(checkpointFolder)

ssc.start()

// This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
ssc.awaitTerminationOrTimeout(streamingContextTimeout) // time to wait in milliseconds; and/or run stopSparkContext above
// ssc.awaitTermination()


// == FORCE STOP ==
// StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
