// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import timeOp.TimeCommon._
import genome.util.DataPath.getPath

// self defined libs

/**
* grab one agents day to day trip and see whether we can cluster the trip or not
* choosing period: 2019-10-01 - 10-07
* @author: jingxuan
* @date: 18/5/20
* @project: analysis
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val agent = "1467CFB4497C648D0BAF448C3CB0519A30CCDED1D7B32F71669780F4AA20217D"

val dates = dateRange("2019-10-01", "2019-10-10", 1).map(_.replace("-", ""))

val paths = dates.map(d => getPath(spark = spark, date = d, module = "trip")).flatMap(x=>x)

val trips = paths.map{p =>
  val trip = spark.read.load(p)
  val selectedTrip = trip.where($"agentId" === agent)
  selectedTrip
}.reduce(_ union _)

trips.select("startGeoUnitId", "endGeoUnitId", "startTime", "endTime", "trajectory", "listOfLinks").write.parquet("s3a://au-daas-users/jingxuan/TEST/link/201910")

// analysis

// saving result