// scala default libs

// other independency
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import toolkits.SparkBin.customizeBin

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 28/7/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
def readBatches(start: Long, end: Option[Long], inputPath:String, period:Int = 0, interval:Int = 300): DataFrame= {
  val numBatches =  if(end.isEmpty) period/interval else (end.get - start)/interval
  val batchInputPaths = (0L to numBatches - 1L).map(i => (i, inputPath+(start+i*interval).toString+"000"))
  val df = batchInputPaths.map{i => {
    try{
      spark.read.load(i._2)
    }catch{
      case e:Exception =>
        println(i._2)
        spark.read.load(i._2)
    }}}.reduce(_.union(_))
  df
}
val sim = readBatches(1595030400, Some(1595116800),"hdfs:///user/yasara/rstay/output_jul_17_18/*")
//val prod = readBatches(1594915500, Some(1595030400), "hdfs://ds-pro-head-02.dataspark.net:8020/user/jingxuan/TEST/rtsp_v2_debug/result_")
val prod = readBatches(1595030400, Some(1595116800), "hdfs:///user/jingxuan/TEST/rtsp_v2_debug/result_")

sim.agg(max("reportingTime"), min("reportingTime")).show(false)
prod.agg(max("reportingTime"), min("reportingTime")).show(false)

sim.count() // 1230897676
prod.count() // 1405560771

sim.show(10,false)
prod.show(10, false)

val agent = "525016B30D3E5B524F5619D046BA05DB3CD64C"
val spCond = ($"curIsStayPoint" === "Yes") || ($"prevIsStayPoint" === "Yes")
sim.where(spCond && $"agentId" === agent).count()
prod.where(spCond && $"agentId" === agent).count()


val prodCount = prod.where(spCond).groupBy($"agentId").count()
prodCount.write.csv("hdfs:///user/jingxuan/TEST/rtsp_prod2")

val simCount = sim.where(spCond).groupBy($"agentId").count()
simCount.write.csv("hdfs:///user/jingxuan/TEST/rtsp_sim")

val simCount1 = spark.read.csv("hdfs:///user/jingxuan/TEST/rtsp_sim").toDF("agentId", "count")
val prodCount1 = spark.read.csv("hdfs://ds-pro-head-02.dataspark.net:8020/user/jingxuan/TEST/rtsp_prod2").toDF("agentId", "prodCount")

val res = simCount1.join(prodCount1, Seq("agentId")).withColumn("diff", $"count" - $"prodCount").withColumn("compare", when($"diff" >0, "More").when($"diff" < 0, "Less").otherwise("Equal"))

val bin_res = customizeBin(Array(-300.0, -250.0, -200.0, -150.0, -100.0, -50.0, 0.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0), "diff", "binDiff", res)
bin_res.groupBy("binDiff").count.sort("binDiff").show(15, false)
Array(-300.0, -250.0, -200.0, -150.0, -100.0, -50.0, 0.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0).zipWithIndex
// analysis
/**
|diff|  count|
+----+-------+
|   M|1382002|
|   L|1083625|
|   E| 722257|
*/

/**
|summary|              diff|
+-------+------------------+
|  count|           3187884|
|   mean|-9.261793089083543|
| stddev| 41.30264951401269|
|    min|            -287.0|
|    max|             287.0|
  */

/**
Array((-300.0,0), (-250.0,1), (-200.0,2), (-150.0,3), (-100.0,4), (-50.0,5), (0.0,6), (50.0,7), (100.0,8), (150.0,9), (200.0,10), (250.0,11), (300.0,12))

|binDiff|count  |
+-------+-------+
|0.0    |681    |
|1.0    |2459   |
|2.0    |5918   |
|3.0    |28988  |
|4.0    |557301 |
|5.0    |488278 |
|6.0    |2007268|
|7.0    |72104  |
|8.0    |14730  |
|9.0    |6528   |
|10.0   |2854   |
|11.0   |775    |
  */
// saving result