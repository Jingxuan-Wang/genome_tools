// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 26/6/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val sp = spark.read.load("/data/prod-realtime/rstay-test/result_1593071700000")

val test = sp.withColumn("geoUnitId", when($"curIsStayPoint" === "Yes", col("curGeoUnitId")).when($"prevIsStayPoint" === "Yes", col("prevGeoUnitId")).otherwise(lit(null))).withColumn("cellList", when($"curIsStayPoint" === "Yes", col("curCellList")).when($"prevIsStayPoint" === "Yes", col("prevCellList")).otherwise(lit(null))).select("agentId", "geoUnitId", "cellList", "reportingTime").where($"geoUnitId".isNotNull)
// analysis
test.show(1,false)

// saving result