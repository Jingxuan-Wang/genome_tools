// scala default libs

// other independency
import genome.util.DataPath.getPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 19/5/20
* @project: analysis
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val path = getPath(spark = spark, date = "20191001", module = "trip")

val trip = spark.read.load(path.get)

val agent = "1467CFB4497C648D0BAF448C3CB0519A30CCDED1D7B32F71669780F4AA20217D"
val myTrip = trip.where($"agentId" === agent && $"startGeoUnitId" === "grid_4433769452151081387_22_151.081387_-33.769452")
// analysis
myTrip.show(false)

// saving result