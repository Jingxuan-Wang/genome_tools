// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* We found the route in one of the trips are absolutely wrong since some of consecutive links are far from
* each other. In this notebook, we are trying to verify the routes from route Option
*
* @author: jingxuan
* @date: 20/5/20
* @project: Debug
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

val startSa1 = "12601149506"
val endSa1 = "12601149537"
// loading data
val routes = spark.read.load("s3a://au-daas-compute/bootstrapScripts/meta-input/routeMagic/route-options/groupedroutes20191211.parquet")

routes.printSchema()
// analysis
val result = routes.where($"startSA1" === startSa1 && $"endSA1" === endSa1 )
result.select("candidate").show(false)


// saving result