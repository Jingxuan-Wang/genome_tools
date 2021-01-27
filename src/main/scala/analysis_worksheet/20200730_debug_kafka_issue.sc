// scala default libs

// other independency
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import timeOp.TimeCommon._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 30/7/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
//val batch = 1589991300L
//val proPath = "hdfs:///user/jingxuan/RES/rtsp/lbs_sim/time="
//val conPath = "hdfs:///user/jingxuan/TEST/rtsp_debug/result_"
//def readBatch(batch: Long, conPath: String, proPath: String): Long = {
//  val con = spark.read.option("delimiter", "|").csv(conPath+batch.toString+"000")
//  val pro = spark.read.option("delimiter", "|").csv(proPath+(batch+300L).toString+"000")
//  con.show(1,false)
//  pro.show(1,false)
//  val conCount = con.distinct.count()
//  val proCount = pro.distinct.count()
//  conCount - proCount
//}
//
//val con = spark.read.option("delimiter", "|").csv(conPath+"1589991300"+"000")
//
//val diff = (1589991600L to 1589998800L by 300L).map(readBatch(_, conPath, proPath))
//// analysis
//
//val con = spark.read.load("/user/yasara/rtsp_stat/n5_sub_jul_17_18/sum_stat")
//val pro = spark.read.load("/user/yasara/rtsp_stat/n5_pub_jul_17_18/sum_stat")
//// saving result
//val con2 = con.withColumnRenamed("distcount", "conDistCount").withColumnRenamed("count", "conCount")
//val pro2 = pro.withColumnRenamed("distcount", "proDistCount").withColumnRenamed("count", "proCount")
//val res = pro2.join(con2, Seq("batchTime")).withColumn("diff", $"conDistCount" - $"proDistCount")
//res.select("diff").describe().show()
//
//val con3 = df2.withColumnRenamed("distcount", "conDistCount").withColumnRenamed("count", "conCount")
//val pro3 = df.withColumnRenamed("distcount", "proDistCount").withColumnRenamed("count", "proCount")
//val res2 = pro3.join(con3, Seq("batchTime")).withColumn("diff", $"conDistCount" - $"proDistCount")
//res2.select("diff").describe().show()


/**
  * Checking the result of dummy producer
*/
val proPath = "hdfs:///user/yasara/realtime/new_fix/parts/jul/17_18/time="
val dumPath = "hdfs:///user/yasara/rstay/output_jul_17_18_mock/result_"

def readBatch(batch: Long, conPath: String, proPath: String): Long = {
  val con = spark.read.parquet(conPath+batch.toString+"000")
  val pro = spark.read.option("delimiter", "|").csv(proPath+batch.toString+"000")
  con.show(1,false)
  pro.show(1,false)
  val conCount = con.distinct.count()
  val proCount = pro.distinct.count()
  conCount - proCount
}

val diffDum = (1594915500L to 1594940400L by 300L).map(i => readBatch(i, dumPath, proPath))

val recCount = spark.read.load("hdfs:///user/jingxuan/TEST/rtsp_v2/batchCount").withColumnRenamed("distincCount", "recDistCount").withColumnRenamed("count", "recCount")
val pubCount = spark.read.load("/user/yasara/rtsp_stat/n6_pub_jul_17_18/sum_stat").withColumnRenamed("distcount", "pubDistCount").withColumnRenamed("count", "pubCount")

val check = recCount.join(pubCount, Seq("batchTime"), "inner").withColumn("diff", $"recDistCount" - $"pubDistCount")
check.select($"diff").describe().show()

val epochTimes = check.select("batchTime").sort("batchTime")
val expected = (1594915200 to 1595088000 by 300).map(i => (i.toString+"000", "E")).toDF("batchTime", "flag")
expected.count

val toDT = udf[]