// scala default libs

// other independency
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import genome.util.DataPath.getPath
import genome.util.{Coordinate, LBSRecord}
import genome.util.LBSRecord._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Window

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 29/10/20
* @project: greenfield
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val lbs_path = getPath(spark, "20201028", "lbs").get

val df = sc.textFile(lbs_path).map(LBSRecord.fromString(_, "Australia/Sydney")).flatMap(x=>x).toDF()

val w = Window.partitionBy("agentID").orderBy("datetime").rowsBetween(-1, 1)

df.show(3,false)

val df1 = df.where($"datetime" >= "2020-10-28 10:00:00").withColumn("coord", struct($"lat", $"lon").as[Coordinate]).withColumn("coords", collect_list("coord").over(w))

df1.show(5,false)


def bearing(pointA: Coordinate, pointB: Coordinate): Double = {
  val lat1: Double = math.toRadians(pointA.lat)
  val lon1: Double = math.toRadians(pointA.lon)
  val lat2: Double = math.toRadians(pointB.lat)
  val lon2: Double = math.toRadians(pointB.lon)
  val dLon = lon2 - lon1

  val y = math.sin(dLon) * math.cos(lat2)
  val x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math
    .cos(dLon)

  math.atan2(y, x)
}


def bearingDegrees(pointA: Coordinate, pointB: Coordinate): Double = math.toDegrees(bearing(pointA, pointB))


def calculateBearing(pointA: Coordinate, pointB: Coordinate): Double = {
  val rawDegree = bearingDegrees(pointA, pointB)

  if (rawDegree >= 0) rawDegree else 360 + rawDegree
}

def trigonalBearingAbsoluteDifference(updates: Seq[GenericRowWithSchema]): Double = {
  updates.map(r => Coordinate(lat = r.getAs[Double]("lat"), lon = r.getAs[Double]("lon"))) match {
    case Seq(x, y, z) =>
      scala.math.abs(calculateBearing(x, y) - calculateBearing(y, z))
    case Seq(x, y) => 0.0
    case Seq(x) => 0.0
  }
}

val bearingUDF = udf[Double, Seq[GenericRowWithSchema]](trigonalBearingAbsoluteDifference)

val df2 = df1.withColumn("bearing_degree", bearingUDF($"coords")).withColumn("oscillation", when($"bearing_degree" > 120, true).otherwise(false))

df2.where($"oscillation").write.mode(SaveMode.Overwrite).parquet("hdfs:///user/jingxuan/RES/tr_20201028_oscillation")
// analysis

// saving result