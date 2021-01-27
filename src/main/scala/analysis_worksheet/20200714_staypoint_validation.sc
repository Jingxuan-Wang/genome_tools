// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 14/7/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val sp1 = spark.read.load("hdfs:///user/yasara/rstay/output_jx3/*_1589997000000")
val sp2 = spark.read.load("hdfs:///user/yasara/rstay/output_jx3/*_1590010800000")
val sp3 = spark.read.load("hdfs:///user/yasara/rstay/output_jx3/*_1590012300000")

val sp11 = sp1.select($"agentId", $"reportingTime".as("repTime1"), $"lastObserveTime".as("obsTime1"))
val sp22 = sp1.select($"agentId", $"reportingTime".as("repTime2"), $"lastObserveTime".as("obsTime2"))
val sp33 = sp1.select($"agentId", $"reportingTime".as("repTime3"), $"lastObserveTime".as("obsTime3"))

val res1 = sp11.join(sp22, "agentId")
val res2 = sp22.join(sp33, "agentId")




//val df = spark.read.load("hdfs:///user/jingxuan/TEST/rtsp_v2_park2/result_*")
val df = spark.read.load("hdfs:///user/yasara/rstay/output_jx3/*")
val df_select = df.select("agentId", "reportingTime", "lastObserveTime")
val res = df_select.groupBy("agentId").agg(countDistinct("lastObserveTime").as("count"))
res.select("count").describe().show()


val df1 = spark.read.load("hdfs:///user/jingxuan/TEST/rtsp_v2/result_15946*")
val df2 = spark.read.load("hdfs:///user/jingxuan/TEST/rtsp_v2/result_15947*")
val df_prod = df1.union(df2)
val df_select_prod = df_prod.select("agentId", "reportingTime", "lastObserveTime")
val res_prod = df_select_prod.groupBy("agentId").agg(countDistinct("lastObserveTime").as("count"))
res_prod.select("count").describe().show()

