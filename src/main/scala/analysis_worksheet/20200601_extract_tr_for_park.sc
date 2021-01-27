// scala default libs

// other independency
import genome.util.Trajectory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, collect_list}

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 1/6/20
* @project: realtime
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val rtsp = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/rstay-test/result_15919*/*")

rtsp.printSchema()

rtsp.select($"status").show(1,false)

rtsp.where($"status" === "Stay").show(3,false)

rtsp.where($"agentId" === "414010D11ACA700C9E2FF7AA7185C7E4FB5AC9" && $"geoUnitId")