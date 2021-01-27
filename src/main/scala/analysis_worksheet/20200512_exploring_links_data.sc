// scala default libs

// other independency
import org.apache.spark.sql.{DataFrame, SparkSession}
import timeOp.TimeCommon._

// self defined libs

/**
* Looking at my link data to see how regular the computation could be for home-work trips
*
* @author: jingxuan
* @date: 12/5/20
* @project: analysis
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data

def mineTrip(sdate:String, edate:String, agent: String): DataFrame = {
  val dates = dateRange(sdate, edate, 1).map(d => d.replace("-", ""))
  val trip = dates.map(d => spark.read.load(s"s3a://au-daas-compute/output/parquet/trip/$d/In").filter($"agentId" === agent)).reduce(_ union _)
  val selectedTrip = trip.select($"startGeoUnitId", $"endGeoUnitId", $"listOfLinks", $"startTime", $"endTime")
  selectedTrip
}