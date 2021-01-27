// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 22/6/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val p0 = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/rstay-test/result_1592559600000")
val p1 = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/rstay-test/result_1592559900000")
val p2 = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/rstay-test/result_1592560200000")
val p3 = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/rstay-test/result_1592560500000")
val p4 = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/rstay-test/result_1592560800000")
val p5 = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/rstay-test/result_1592561100000")
val p6 = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/rstay-test/result_1592561400000")

// analysis
p0.count()
p0.where($"isStayPoint" === "Yes").count()

p1.count()
p1.where($"isStayPoint" === "Yes").count()

p2.count()
p2.where($"isStayPoint" === "Yes").count()

p3.count()
p3.where($"isStayPoint" === "Yes").count()

p4.count()
p4.where($"isStayPoint" === "Yes").count()

p5.count()
p5.where($"isStayPoint" === "Yes").count()
// saving result