// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 26/5/20
* @project: analysis
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val hw03 = spark.read.load("s3a://au-daas-compute/bootstrapScripts/meta-input/homework/202003")

val agent = "1467CFB4497C648D0BAF448C3CB0519A30CCDED1D7B32F71669780F4AA20217D"

hw03.filter($"agent_id" === agent).show(1, false)

val hw02 = spark.read.load("s3a://au-daas-compute/bootstrapScripts/meta-input/homework/202002")
hw02.filter($"agent_id" === agent).show(1, false)

val hw01 = spark.read.load("s3a://au-daas-compute/bootstrapScripts/meta-input/homework/202001")
hw01.filter($"agent_id" === agent).show(1, false)
// analysis

// saving result