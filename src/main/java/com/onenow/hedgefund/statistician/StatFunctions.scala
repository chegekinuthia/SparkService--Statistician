package com.onenow.hedgefund.statistician

import com.onenow.hedgefund.discrete.{DataTiming, DataType}
import com.onenow.hedgefund.event.PairActivity
import com.onenow.hedgefund.lookback.TradingLookback
import com.onenow.hedgefund.util.Piping
import org.apache.spark.rdd.RDD

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
  // PairActivity -> ((serieName,sectorName,lookback) (d,d,d,d,d,d,d,d)) for every lookback window
  val getWindowValuesFromPairActivityList = (event:PairActivity, lookbacks:List[TradingLookback]) => {

    val value = event.getStoredValue.toDouble   //PART 2: 1
    val sum = value                             //PART 2: 2
    val count = 1.0                             //PART 2: 3
    val mean = value / count                    //PART 2: 4
    val deviation = 0.0                         //PART 2: 5
    val sumOfSquaredDeviations = 0.0            //PART 2: 6
    val timeInMsec:Long = event.getTimeInMsec   //PART 2: 7

    import scala.collection.mutable.ListBuffer
    val items = ListBuffer[((String,String,String,String),(Double,Double,Double,Double,Double,Double,Long))]()

    for(lookback <- lookbacks) {  // expand to one record per window

      val serieName = event.getSerieName                    //PART 1: 1
      val sectorName = event.getSectorName.toString         //PART 1: 2
      val windowSec = lookback.getWindowSec.toString        //PART 1: 3
      val slideSec = lookback.getSlideIntervalSec.toString  //PART 1: 4

      val toAdd = ((serieName, sectorName, windowSec, slideSec),                      //PART 1
        (value, sum, count, mean, deviation, sumOfSquaredDeviations, timeInMsec))     //PART 2

      items += toAdd
    }
    items.toList
  }
  def getLatestTimestamp (timeInMsec1:Long, timeInMsec2:Long):Long = {
    var timeInMSec1to2 = timeInMsec1
    if(timeInMsec2>timeInMSec1to2) {
      timeInMSec1to2 = timeInMsec2
    }
    return timeInMSec1to2
  }
  // STRUCTURED STREAMING
  // https://spark.apache.org/docs/latest/streaming-programming-guide.html
  // https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/10%20Window%20Aggregations.html
  // http://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=reducebykey
  // https://stackoverflow.com/questions/24071560/using-reducebykey-in-apache-spark-scala
  // https://stackoverflow.com/questions/28147566/custom-function-inside-reducebykey-in-spark
  // sort rdd https://stackoverflow.com/questions/32988536/spark-dstream-sort-and-take-n-elements
  //
  // associative calculations in ReduceByKeyAndWindow
  val addEventToStats = (accumulator:(Double,Double,Double,Double,Double,Double,Long),
                         current:(Double,Double,Double,Double,Double,Double,Long)) => {
    val currentValue = current._1
    val valueTotal = accumulator._2 + current._2
    // stats
    val countTotal = accumulator._3 + current._3
    val meanTodate = valueTotal / countTotal            // for easy visual inspection
    val currentDeviation = currentValue - meanTodate
    val sumOfSquaredDeviations = accumulator._6 + currentDeviation * currentDeviation
    // time
    var timeInMsec = accumulator._7
    if(current._7>timeInMsec) {
      timeInMsec = current._7
    }

    (currentValue, valueTotal, countTotal, meanTodate, currentDeviation, sumOfSquaredDeviations,timeInMsec)
  }
  val subtractEventsFromStats = (accumulator:(Double,Double,Double,Double,Double,Double,Long),
                                 current:(Double,Double,Double,Double,Double,Double,Long)) => {
    val currentValue = current._1
    val valueTotal = accumulator._2 - current._2
    // stats
    val countTotal = accumulator._3 - current._3
    val meanTodate = valueTotal / countTotal          // for easy visual inspection
    val currentDeviation = currentValue - meanTodate
    val sumOfSquaredDeviations = accumulator._6 - currentDeviation * currentDeviation
    // time
    var timeInMsec = accumulator._7
    if(current._7>timeInMsec) {
      timeInMsec = current._7
    }

    (currentValue, valueTotal, countTotal, meanTodate, currentDeviation, sumOfSquaredDeviations, timeInMsec)
  }
  // EMIT OUTPUT
  // Emit Row
  val emitStats = (entry: ((String,String,String,String),(Double,Double,Double,Double,Double,Double,Long))) => {
    try {
      println(entry)

    } catch {
      case e: Exception => // e.printStackTrace()
    }
  }
  val emitCoStats = (entry: ((String,String,String,String),(Double,Double,Long))) => {
    try {
      println(entry)

      // TODO:
      // Register a temp table at every batch interval so that it can be queried separately
      // ctr.window(Duration(60000)).foreachRDD { rdd =>
      //   val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
      //  sqlContext.createDataFrame(rdd).toDF("adId", "clicks", "impressions", "CTR", "Time").registerTempTable("ctr")
      //  rdd.take(1)
      // }

    } catch {
      case e: Exception => // e.printStackTrace()
    }
  }
  // Emit RDD
  val emitRddStats = (statRdd: RDD[((String,String,String,String),(Double,Double,Double,Double,Double,Double,Long))]) => {
    statRdd.collect().foreach(emitStats)
  }
  val emitRddCoStats = (coStatRdd: RDD[((String,String,String,String),(Double,Double,Long))]) => {
    coStatRdd.collect().foreach(emitCoStats)
  }
}

