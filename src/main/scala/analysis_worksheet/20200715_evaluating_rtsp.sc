// scala default libs

// other independency
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 15/7/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

def readBatches(start: Long, end: Option[Long], inputPath:String, period:Int = 0, interval:Int = 300): DataFrame= {
  val numBatches =  if(end.isEmpty) period/interval else (end.get - start)/interval
  val batchInputPaths = (0L to numBatches - 1L).map(i => (i, inputPath+(start+i*interval).toString+"000"))
  val df = batchInputPaths.map{i => {
    try{
      spark.read.load(i._2).withColumn("batchNo", lit(i._1))
    }catch{
      case e:Exception =>
        println(i._2)
        spark.read.load(i._2).withColumn("batchNo", lit(i._1))
    }}}.reduce(_.union(_))
  df
}


val df = readBatches(start=1589991000, end=Some(1589995500),inputPath = "hdfs:///user/jingxuan/TEST/rtsp_v2_0716/result_")

// check the impact of out of range data
val outRange = df.withColumn("timeDiff", to_timestamp($"reportingTime").cast(LongType) - to_timestamp($"lastObserveTime").cast(LongType)).withColumn("beforePeriod", when($"timeDiff" > $"batchNo" * 300, "Y").otherwise("N"))
outRange.groupBy($"beforePeriod").count().show()


// check staypoint duration
val dur = df.withColumn("duration", when($"curIsStayPoint" === "Yes", $"curDuration").when($"prevIsStayPoint" === "Yes", $"prevDuration").otherwise(lit(-1)))
dur.where($"duration" > 0).select("duration").describe().show()

// check lastObserveTime
val obs = df.groupBy("agentId").agg(countDistinct("lastObserveTime").as("count"))
obs.select("count").describe().show()

// saving result

val v0 = readBatches(start=1589990400, end=Some(1589996100), inputPath = "hdfs:///user/jingxuan/TEST/rstay-test/result_")

v0.show(10,false)

val outRange = df.withColumn("timeDiff", to_timestamp($"reportingTime").cast(LongType) - to_timestamp($"lastObserveTime").cast(LongType))

outRange.select("timeDiff").describe().show()
