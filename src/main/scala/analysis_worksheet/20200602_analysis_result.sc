// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import geojson.{Feature, FeatureCollection}
import geojson.GeoJsonProtocol._
import com.esri.core.geometry.Point
import spray.json._

import scala.io.Source

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 2/6/20
* @project: RTSP
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

case class StayPoint(
    agent_id:String,
    lat: Double,
    lon: Double,
    stime: String,
    etime: String,
    duration: Double,
    radius: Double,
    status: String)

// loading data
val sp = sc.textFile("hdfs:///user/jingxuan/TEST/rtsp_for_park")

// analysis
val res = sp.map(r => r.replace("[", "").replace("]", ""))

res.take(1)

val clean_res = res.map(r => r.split(", ")).flatMap(x=>x)

clean_res.repartition(1).saveAsTextFile("hdfs:///user/jingxuan/TEST/rtsp_for_park_f")

// saving result
val sp2 = sc.textFile("hdfs:///user/jingxuan/TEST/rtsp_for_park_f")

def parse(r: String): Option[StayPoint] = {
  val fields = r.split(",")
  try{
    Some(StayPoint(
      agent_id = fields(0),
      lat = fields(1).toDouble,
      lon = fields(2).toDouble,
      stime = fields(3),
      etime = fields(4),
      duration = fields(5).toDouble,
      radius = fields(6).toDouble,
      status = fields(7)
    ))
  }catch{
    case e:Exception => None
  }
}

val valid_records = sp2.map(r => parse(r)).filter(_.isDefined).map(_.get)
valid_records.toDF().repartition(1).write.parquet("hdfs:///user/jingxuan/TEST/rtsp_parquet")
valid_records.count()

val parkShape: FeatureCollection = Source.fromFile("/home/jingxuan/bukit_timah_park.geojson").mkString.parseJson.convertTo[FeatureCollection]

val parkShapeBD = sc.broadcast(parkShape)

def selectSP(lat: Double, lon:Double, polygon: FeatureCollection): String = {
  val point = new Point(lon, lat)
  try{
    polygon.find(f => f.geometry.contains(point)) match {
      case Some(x: Feature) => "1"
      case _ => "2"
    }
  }catch{
    case e: Exception => "3"
  }
}

val spInShape = valid_records.map(st => selectSP(st.lat, st.lon, parkShapeBD.value))

spInShape.toDF("match").groupBy("match").count().show(false)

spInShape.take(2)