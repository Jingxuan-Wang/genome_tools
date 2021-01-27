// scala default libs

// other independency
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 14/7/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val batch = "1590163200"
val lbs = sc.textFile("/user/jingxuan/RES/rtsp/his_lbs/20200523/time="+batch+"000")
val df = lbs.map(x => x.split("\\|")(0)).toDF("dt").withColumn("time", to_timestamp($"dt")).withColumn("batchTime", to_timestamp(from_unixtime(lit(batch)))).withColumn("time_diff", ($"time".cast(LongType) - $"batchTime".cast(LongType))/60)

df.select("time_diff").describe().show()
// analysis
def readBatches(start: Long, end: Option[Long], inputPath:String, period:Int = 0, interval:Int = 300): DataFrame= {
  val numBatches =  if(end.isEmpty) period/interval else (end.get - start)/interval
  val batchInputPaths = (0L to numBatches - 1L).map(i => (i, inputPath+(start+i*interval).toString+"000"))
  val df = batchInputPaths.map(i =>
      sc.textFile(i._2).map(x => x.split("\\|")(0)).toDF("dt").withColumn("time", to_timestamp($"dt")).withColumn("batchTime", to_timestamp(from_unixtime(lit(start+i._1*interval)))).withColumn("time_diff", ($"time".cast(LongType) - $"batchTime".cast(LongType))/60)).reduce(_.union(_))
  df
}

val lbs_df = readBatches(1590019200, Some(1590105600), "hdfs:///user/jingxuan/RES/rtsp/lbs_sim/time=")

lbs_df.select("time_diff").describe().show()

// saving result
val batch1 = "1593702000"
val lbs1 = sc.textFile("/user/yasara/realtime/part_job/data_03/time="+batch1+"000")
val df1 = lbs1.map(x => x.split("\\|")(0)).toDF("dt").withColumn("time", to_timestamp($"dt")).withColumn("batchTime", to_timestamp(from_unixtime(lit(batch1)))).withColumn("time_diff", ($"time".cast(LongType) - $"batchTime".cast(LongType))/60)

df1.show(10,false)