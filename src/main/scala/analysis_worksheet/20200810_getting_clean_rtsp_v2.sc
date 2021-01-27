// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 10/8/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
def cleanRTSP(batchNum: Long, inputPath: String, outputPath: String): Unit = {
  val df = spark.read.load(inputPath+batchNum.toString+"000").withColumn("timeDiff", to_timestamp($"reportingTime").cast(LongType) - to_timestamp($"lastObserveTime").cast(LongType))
  val durCond = $"timeDiff" <= 14400
  val cleanedDF = df.where(durCond).drop($"timeDiff")
  cleanedDF.write.parquet(outputPath+batchNum.toString+"000")
}

// analysis
(1596294000L to 1596383700L by 300L).map(i => cleanRTSP(i, "hdfs:///user/jingxuan/TEST/rtsp_v2_debug/result_", "hdfs:///user/jingxuan/TEST/rtsp_v2_cleaned/result_"))
(1596384000L to 1596902400L by 300L).map(i => cleanRTSP(i, "hdfs:///user/jingxuan/TEST/rtsp_v2_debug/result_", "hdfs:///user/jingxuan/TEST/rtsp_v2_cleaned/result_"))

(1593529200L to 1594396800L by 300L).map(i=>cleanRTSP(i, "hdfs:///data/prod-realtime/rstay-test/result_", "hdfs:///user/jingxuan/TEST/rtsp_v2_cleaned/result_"))
(1594397100L to 1595174400 by 300L).map(i=>cleanRTSP(i, "hdfs:///data/prod-realtime/rstay-test/result_", "hdfs:///user/jingxuan/TEST/rtsp_v2_cleaned/result_"))
// saving result
1594396800 - 1594051500