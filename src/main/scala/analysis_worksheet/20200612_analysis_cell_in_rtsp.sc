// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 12/6/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val cell = spark.read.option("header", "true").csv("hdfs:///user/changhong/realtimeSG/ambient/parks_cells_ratio.csv")
val

// analysis

// saving result