// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import geojson.{Feature, FeatureCollection}
import geojson.GeoJsonProtocol._
import com.esri.core.geometry.Point
import spray.json._
import genome.util.{Trajectory, Update}

import scala.io.Source
import timeOp.TimeCommon.{parseToUTC,zdtToString}

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 3/6/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val sp1 = sc.textFile("hdfs:///user/jingxuan/TEST/rtsp_for_park/20200515/part1")
val sp2 = sc.textFile("hdfs:///user/jingxuan/TEST/rtsp_for_park/20200515/part2")
val sp3 = sc.textFile("hdfs:///user/jingxuan/TEST/rtsp_for_park/20200515/part3")
val sp4 = sc.textFile("hdfs:///user/jingxuan/TEST/rtsp_for_park/20200515/part4")
val sp5 = sc.textFile("hdfs:///user/jingxuan/TEST/rtsp_for_park/20200515/part5")
val sp6 = sc.textFile("hdfs:///user/jingxuan/TEST/rtsp_for_park/20200515/part6")
val sp = sc.union(Seq(sp1, sp2, sp3, sp4, sp5, sp6))

case class StayPoint(
                      agent_id:String,
                      lat: Double,
                      lon: Double,
                      stime: String,
                      etime: String,
                      duration: Double,
                      radius: Double,
                      status: String)

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

// analysis

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

val spInShape = sp.map(r => parse(r)).filter(_.isDefined).map(_.get).map(st => selectSP(st.lat, st.lon, parkShapeBD.value))

spInShape.toDF("match").groupBy("match").count().show(false)

// saving result

val tr = sc.textFile("hdfs:///user/jingxuan/TEST/trajectory/20200515")

def trFromString(s: String): Trajectory = {
  val fields = s.split(";")
  val agentId = fields(0)
  val updates = fields.drop(1).map(upFromString).toSeq
  val tr = new Trajectory(agentId, updates)
  tr
}

def upFromString(s: String): Update = {
  val fields = s.split(",")
  Update(
    lon = fields(0).toDouble,
    lat = fields(1).toDouble,
    timestamp = fields(2),
    cellType = CellType.withName(fields(4)),
    cell = fields(3)
  )
}
val res = tr.map(trFromString(_)).map(r => new Trajectory(r.agentId, r.updates.sortBy(_.timestamp))).map(r => r.agentId+";"+r.updates.map(_.toString).mkString(";"))

res.saveAsTextFile("hdfs:///user/jingxuan/TEST/trajectory_for_testing/20200515")

val tr2 = sc.textFile("hdfs:///user/jingxuan/TEST/trajectory_for_testing/20200515")
val res2 = tr2.map(trFromString(_)).map{r =>
  val agentId = r.agentId
  val updatesNew = r.updates.map(u => u.copy(timestamp = zdtToString(parseToUTC(u.timestamp, "Asia/Singapore"))))
  new Trajectory(agentId = agentId, updates = updatesNew)
}.map(t => t.agentId+";"+t.updates.map(u=> Seq(u.lat.toString, u.lon.toString, u.timestamp, u.cellType, u.cell).mkString(",")).mkString(";"))

res2.take(1)

res2.saveAsTextFile("hdfs:///user/jingxuan/TEST/trajectory_for_testing_f/20200515")
