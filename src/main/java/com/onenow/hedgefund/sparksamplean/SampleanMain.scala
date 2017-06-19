// IMPORT THIRD PARTY
import com.amazonaws.regions._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.util.json.JSONObject

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
val serviceType = "REPLAY";  // This determines the name of the stream that will be used
val deployEnv = "STAGING";

val region = "us-east-1" // Kinesis.defaultRegion
val endPoint = "https://kinesis.us-east-1.amazonaws.com"

val AWS_ACCESS_KEY_ID = "AKIAJ2TWLESHWGQDXTEA"
val AWS_SECRET_KEY = "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"

val streamName = serviceType.toString + "-" + deployEnv
// val kinRead = new Kinesis(serviceType, deployEnv, region, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY);
// val kinesisClient = kinRead.getClient

val numShards = 1
// val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size

val initialPosition = InitialPositionInStream.TRIM_HORIZON // LATEST, TRIM_HORIZON, AT_TIMESTAMP



// CONFIGURE THE STREAMING CONTEXT
val batchIntervalSec = 5

StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }  // stop the streaming contexts without stopping the spark context
val ssc = new StreamingContext(sc, Seconds(batchIntervalSec)) // use in databricks


// OPERATIONS: in every microbatch get the union of shard streams
// https://spark.apache.org/docs/latest/streaming-kinesis-integration.html
// https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/streaming/kinesis/KinesisUtils.html
// 
// Create the Kinesis DStreams
val kinesisStreams = (0 until numShards).map { i =>
  KinesisUtils.createStream(ssc, serviceType.toString, streamName, endPoint, 
      region.toString, initialPosition, Milliseconds(batchIntervalSec*1000), StorageLevel.MEMORY_AND_DISK_2,
      AWS_ACCESS_KEY_ID, AWS_SECRET_KEY)
}

// // Union all the streams, each line is Array[Byte]
val unionStreams = ssc.union(kinesisStreams)
// unionStreams.print()

val recordJsons = unionStreams.map(byteArray => new String(byteArray))
recordJsons.print()

ssc.start()

// This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
ssc.awaitTerminationOrTimeout(batchIntervalSec * 100 * 1000)
// ssc.awaitTermination()



