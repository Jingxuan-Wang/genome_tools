// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import timeOp.TimeCommon._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 26/5/20
* @project: realtime
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

case class LBS(
    timestamp: String,
    imsi: String,
    cellid: String,
    lat: Double,
    lon: Double
              )

def fromString(s: String): Option[LBS] = {
  val fields = s.split("\\|")
  try{
    Some(LBS(timestamp = parseToUTC(fields(0), timeZone = "Asia/Singapore"),
      imsi = fields(1),
      cellid = fields(3),
      lat = fields(5).toDouble,
      lon = fields(6).toDouble))
  } catch {
    case e: Throwable => println(s)
      None
  }
}

// loading data
val lbs = sc.textFile("s3a://au-daas-users/jingxuan/RES/lbs_for_rstay_20200520.txt")
lbs.count()

val lbsDataset = lbs.map(fromString(_)).flatMap(x => x)

val trasform = lbsDataset.map(r => (r.imsi, List((r.timestamp, r.lat, r.lon, r.cellid, "1")))).reduceByKey(_ ++ _).map(r => r._1+";"+r._2.sortBy(_._1).map(i => i._3.toString+","+i._2.toString+","+i._1+","+i._4+","+i._5).mkString(";"))

trasform.count()

trasform.repartition(1).saveAsTextFile("s3a://au-daas-users/jingxuan/RES/trajectory_for_rstay_20200520")
// saving result