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
import com.onenow.hedgefund.lookback.{LookbackFactory, TradingLookback}
import com.onenow.hedgefund.integration.SectorName
import com.onenow.hedgefund.statistician.StatFunctions
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


// == SCALA
import scala.collection.JavaConversions._
import collection.mutable._


object StatisticianMain {


  // == WINDOW LOOKBACKS ==
  val factory = new LookbackFactory()
  val lookbacks = factory.getFast

  // == INSTANTIATE KINESIS ==
  // https://docs.databricks.com/spark/latest/structured-streaming/kinesis.html
  val numShards = 1
  val serviceType = ServiceType.STATISTICIAN
  val appName = ServiceType.REPLAY.toString;
  val deployEnv = DeployEnv.STAGING;
  val region = "us-east-1"
  // Kinesis.defaultRegion
  val endPoint = "https://kinesis.us-east-1.amazonaws.com"
  val inputStreamName = ServiceType.REPLAY.toString + "-" + deployEnv.toString


  // == TIMING ==
  // streams:
  val initialPosition = InitialPositionInStream.TRIM_HORIZON
  // LATEST, TRIM_HORIZON, AT_TIMESTAMP
  // context:
  val contextBatchSec = 5
  val contextBatchDuration = Seconds(contextBatchSec)
  // windowing:
  val batchMultiple = 10
  val windowLength = Seconds(contextBatchSec * batchMultiple)
  val slideInterval = Seconds(contextBatchSec)
  // checkpoint:
  val streamCheckpointInterval = Milliseconds(contextBatchSec * 1000)
  val contextRemember = Minutes(1)
  val checkpointFolder = "/Users/Shared/"
  // timeout:
  val streamingContextTimeout = 60L * 1000


  // == CONFIGURE THE STREAMING CONTEXT ==
  // when using spark-submit:
  // Spark context available as 'sc' (master = local[4], app id = local-1497911109319).
  // Spark session available as 'spark'
  StreamingContext.getActive.foreach {
    _.stop(stopSparkContext = false)
  }
  // stop the streaming contexts without stopping the spark context
  @transient
  val sc = SparkContext.getOrCreate()
  @transient
  val ssc = new StreamingContext(sc, contextBatchDuration)
  // use in databricks
  @transient
  val spark = SparkSession.builder().getOrCreate()


  def main(args: Array[String]): Unit = {


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
      .map(StatFunctions.getDeserializedPairActivity)  // TODO: convert to flatMap for when deserialization fails
      .filter(r => StatFunctions.isDataType(r, DataType.PRICE))
      .filter(r => StatFunctions.isDataTiming(r, DataTiming.REALTIME))
      .flatMap(r => StatFunctions.getWindowValuesFromPairActivityList(r, lookbacks.toList))
      ).cache()
    // eventValuesAllWindowsDstream.print()

    // PERFORMANCE EVALUATION
    // varying numTasks in reduceByKeyAndWindow
    // reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks])
    val numReduceTasks = 2;

    // Get the stats of each serie for every window
    val windowStatsDstreamList = (lookbacks.toList.map(lookback => {
      eventValuesAllWindowsDstream
        .filter(r => r._1._3.equals(lookback.getWindowSec.toString))  // for each lookback: process only items flatmapped for that window
        .reduceByKeyAndWindow(                                        // key not mentioned
        StatFunctions.addEventToStats,                              // Adding elements in the new batches entering the window
        StatFunctions.subtractEventsFromStats,                      // Removing elements from the oldest batches exiting the window
        Seconds(lookback.getWindowSec),                             // Window duration
        Seconds(lookback.getSlideIntervalSec),                      // Slide duration
        numReduceTasks
      )
    }.cache()
    )
      )
    // NOTE: reduce(func) to Return a new DStream of single-element RDDs


    // == BEFORE JOIN, TRANSFORM STREAM
    // for the join two dstream have to have the same key... (k,v1).join(k,v2)
    import scala.collection.mutable.ListBuffer

    val windowCoStatsDstreamJoinList = ListBuffer[DStream[((String,String),(Double,Double,Double,Double,Double,Double,Long,String))]]()

    for(stream <- windowStatsDstreamList) {
      val windowDstreamToJoin = stream.transform(rdd => {
        rdd.map(item=> {
          val key = (item._1._3, item._1._4) // (window,slide) removed serieName from key to make joins possible
          val value = (item._2._1, item._2._2, item._2._3, item._2._4, item._2._5, item._2._6, item._2._7, item._1._1) // (d,d,d,d,d,d,l,serieName) added serieName
          (key,value)
        })
      })
      windowCoStatsDstreamJoinList += windowDstreamToJoin
    }


    // == STREAMING JOIN ==
    // VARIANCE, COVARIANCE, CORRELATION: study the relationship of any two streams
    // UNBIASED STATISTICS: https://en.wikipedia.org/wiki/Unbiased_estimation_of_standard_deviation

    @transient
    val windowCoStatsDstreamList = ListBuffer[DStream[((String,String,String,String),(Double,Double,Long))]]()

    for(stream1 <- windowCoStatsDstreamJoinList) {    // 1

      for(stream2 <- windowCoStatsDstreamJoinList) {  // 2

        // Some of the DStreams have different slide durations
        var joinDstream = stream1.join(stream2).map(joined => {
          // https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/13%20Joining%20DStreams.html
          // When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W))

          val standardDeviation1 = scala.math.sqrt(joined._2._1._6/(joined._2._1._3-1))          // sumOfSquaredDeviations/(n-1)
          val standardDeviation2 = scala.math.sqrt(joined._2._2._6/(joined._2._2._3-1))

          // [standard definition] variance = sumOfDeviationProducts(1,2)/(countTotal-1)
          val instantCoVariance1to2 = joined._2._1._5 * joined._2._2._5                           // [unit1*unit2], product of deviations

          val coRrelation1to2 = instantCoVariance1to2 / standardDeviation1 / standardDeviation2    // [no unit]

          // time
          val timeInMSec1to2 = StatFunctions.getLatestTimestamp(joined._2._1._7, joined._2._2._7)

          // build
          val key = (joined._2._1._8, joined._2._2._8, joined._1._1, joined._1._2) // (serie1Name,Serie2Name,window,slide)
          val value = (instantCoVariance1to2, coRrelation1to2, timeInMSec1to2)

          val summaryToAdd = (key,value)

          summaryToAdd
        }
        )

        windowCoStatsDstreamList += joinDstream
      }
    }


    // TODO: receiver math
    //    val currentZscore = currentDeviation / standardDeviation
    //    val opportunity = standardDeviation / meanTodate                 // an indication of % volatility


    // == OUTPUT ==
    windowStatsDstreamList.map(statStream => statStream.foreachRDD(StatFunctions.emitRddStats))  // statistics
    windowCoStatsDstreamList.map(coStatStream => coStatStream.foreachRDD(StatFunctions.emitRddCoStats))


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


  }

}
