import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.dataspark.util.GridMapping.getGridFromCoordinate
import com.dataspark.geogrid.grids.ISEA3H
import com.dataspark.genome.mapper.UpdateToGridMapper
import com.dataspark.util.TimeCommon._

// self defined libs

/**
  * Description
  *
  * @author: jingxuan
  * @date: 4/7/20
  * @project: rtsp
  *
  */

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext

import spark.implicits._

// loading data

val rtsp = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/user/jingxuan/prod-realtime/rstay-test/result_1593841800000")

val sp = spark.read.csv("hdfs:///user/jingxuan/TEST/rtsp_for_park_withc/20200702/*").toDF("agentId", "lat", "lon", "startTime", "endTime", "duration", "radius", "status", "cellList")


def toGrid(lat: Double, lon:Double):String = {
  val mapper = UpdateToGridMapper.of(0.0005)
  val grid = new ISEA3H(22)
  val originalProj = "EPSG:4326"
  val thisgridid = getGridFromCoordinate(lat, lon, grid, originalProj)
  mapper.map(lon, lat, thisgridid).get.toLongString
}

val toGridUDF = udf[String, Double, Double](toGrid)


def toReportTime(startTime: String, endTime: String): Seq[String] = {
  val st = parse(startTime, "Asia/Singapore")
  val roundedST = roundDownTime(st, 300)
  val et = parse(endTime, timeZone = "Asia/Singapore")
  val roundedET = roundDownTime(et, 300)
  val diff = timeDifference(roundedST, roundedET) / 300
  (0L to diff+1L).map(i => roundedST.plusSeconds(i*300)).map(zdtToString(_))
}

val tortUDF = udf[Seq[String], String, String](toReportTime)

val spModify = sp.withColumn("geoUnitId", toGridUDF($"lat", $"lon")).withColumn("reportingTime",
  explode(tortUDF($"startTime", $"endTime")))

spModify.select($"agentId", $"status", $"reportingTime", $"geoUnitId", split($"cellList", ":").as
("cellList")).write.parquet("hdfs:///user/jingxuan/TEST/rtsp_pyspark/20200702")
// analysis

rtsp.show(1,false)
