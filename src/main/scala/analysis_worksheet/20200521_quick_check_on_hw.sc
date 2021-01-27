// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 21/5/20
* @project: Debug
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val hw01 = spark.read.load("s3a://au-daas-compute/bootstrapScripts/meta-input/homework/202001")
val geo01 = spark.read.load("s3a://au-daas-compute/output/parquet/aggregated-geo-hierarchy/202001")

val hw02 = spark.read.load("s3a://au-daas-compute/bootstrapScripts/meta-input/homework/202002")
val geo02 = spark.read.load("s3a://au-daas-compute/output/parquet/aggregated-geo-hierarchy/202002")

hw01.printSchema()
geo01.printSchema()

// analysis
val selectedGeo01 = geo01.where($"sa3" === "12504").select($"geo_hierarchy_base_id".as("home"), $"sa3")
val selectedGeo02 = geo02.where($"sa3" === "12504").select($"geo_hierarchy_base_id".as("home"), $"sa3")

val res01 = hw01.join(selectedGeo01, Seq("home"), "inner")
val res02 = hw02.join(selectedGeo02, Seq("home"), "inner")

res01.count
res02.count

hw01.filter($"home" =!= "NA").count
hw02.filter($"home" =!= "NA").count



// saving result