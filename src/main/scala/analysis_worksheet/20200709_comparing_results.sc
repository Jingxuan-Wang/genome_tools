// scala default libs

// other independency
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.parquet.example.data.simple.LongValue
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 9/7/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

def epochTime(longValue: Long): LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(longValue), ZoneId.of("Asia/Singapore"))
// loading data
val new_data = spark.read.load("hdfs:///user/jingxuan/TEST/rtsp_v2/result_1593706200000")
val old_data = spark.read.load("hdfs:///user/jingxuan/TEST/rtsp_v2_debug/result_")

// analysis
epochTime(1593705900L)
old_data.count()
new_data.count()

val imsi = "52501688F7AE329574C2CF9BD1152BE6B30E23"

new_data.where($"agentId" === imsi).show(10,false)
old_data.where($"agentId" === imsi).show(10,false)

new_data.show(3,false)

// saving result