// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import toolkits.SparkBin.customizeBin
import timeOp.TimeCommon._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 29/7/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

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
// loading data
val sim = readBatches(1595030400, Some(1595116800),"hdfs:///user/yasara/rstay/output_jul_17_18_new/result_")

val prod = readBatches(1595030400, Some(1595116800), "hdfs:///user/jingxuan/TEST/rtsp_v2_debug/result_")

sim.agg(max("reportingTime"), min("reportingTime")).show(false)
/*
+------------------------+------------------------+
|max(reportingTime)      |min(reportingTime)      |
+------------------------+------------------------+
|2020-07-18 23:55:00+0000|2020-07-17 23:55:00+0000|
+------------------------+------------------------+
 */
prod.agg(max("reportingTime"), min("reportingTime")).show(false)
/*
+------------------------+------------------------+
|max(reportingTime)      |min(reportingTime)      |
+------------------------+------------------------+
|2020-07-18 23:55:00+0000|2020-07-17 23:45:00+0000|
+------------------------+------------------------+
 */

sim.count() //1017356930
prod.where($"reportingTime" >= "2020-07-17 23:55:00+0000").count() // 1096716707
// 7.23% difference

sim.show(10,false)
prod.show(10, false)

val spCond = ($"curIsStayPoint" === "Yes") || ($"prevIsStayPoint" === "Yes")

val simCount = sim.where(spCond).groupBy($"agentId").count()
simCount.write.csv("hdfs:///user/jingxuan/TEST/rtsp_sim_with_original_config")

val prodCount = prod.where($"reportingTime" >= "2020-07-17 23:55:00+0000").where(spCond).groupBy($"agentId").count()
prodCount.write.csv("hdfs:///user/jingxuan/TEST/rtsp_prod_with_original_config")

// analysis
val simCount1 = spark.read.csv("hdfs:///user/jingxuan/TEST/rtsp_sim_with_original_config").toDF("agentId", "count")
val prodCount1 = spark.read.csv("hdfs://ds-pro-head-02.dataspark.net:8020/user/jingxuan/TEST/rtsp_prod_with_original_config").toDF("agentId", "prodCount")

val res = simCount1.join(prodCount1, Seq("agentId")).withColumn("diff", $"count" - $"prodCount").withColumn("compare", when($"diff" >0, "More").when($"diff" < 0, "Less").otherwise("Equal"))

res.groupBy("compare").count().show()

val bin_res = customizeBin(Array(-300.0, -250.0, -200.0, -150.0, -100.0, -50.0, 0.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0), "diff", "binDiff", res)

bin_res.groupBy("binDiff").count.sort("binDiff").show(15, false)
Array(-300.0, -250.0, -200.0, -150.0, -100.0, -50.0, 0.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0).zipWithIndex
/*
|binDiff|count  |
+-------+-------+
|0.0    |3843   |
|1.0    |13740  |
|2.0    |43156  |
|3.0    |284712 |
|4.0    |491886 |
|5.0    |705966 |
|6.0    |1363263|
|7.0    |160741 |
|8.0    |53755  |
|9.0    |16818  |
|10.0   |6811   |
|11.0   |2275   |
*/
 */
// saving result
res.where($"diff" <= -250).show(10,false)

/*
+--------------------------------------+-----+---------+------+-------+
|agentId                               |count|prodCount|diff  |compare|
+--------------------------------------+-----+---------+------+-------+
|5250163624A476D118C964019848F8ADF26CE2|22   |288      |-266.0|Less   |
|52501674E3D791A11A5E7A80F80F599197DB0A|12   |288      |-276.0|Less   |
|525016A31975373BC70D96792BEACFBDB7C50D|30   |288      |-258.0|Less   |
|525016AFC70691A6381B5746FB9A12280BBBD9|29   |288      |-259.0|Less   |
|525016B82B390FA5927F5E1FB693EC5D0FF96C|29   |288      |-259.0|Less   |
|52501671DBC1633DC71B70A0F7B181C03BFAD0|29   |288      |-259.0|Less   |
|5150218FA2E85E75D59A06B4569193B4B5D0CA|15   |288      |-273.0|Less   |
|515025917B303B4449E5D22D14F578D618B8F7|7    |265      |-258.0|Less   |
|5250169AD75739E2E7DE1F20344A3AB337A9A9|10   |288      |-278.0|Less   |
|525016BBC91AE0D8D2BBA7D632906774EF1223|5    |288      |-283.0|Less   |
+--------------------------------------+-----+---------+------+-------+
 */

val agent = "525016A31975373BC70D96792BEACFBDB7C50D"
sim.filter($"agentId" === agent).show(20, false)

prod.filter($"agentId" === agent).show(20, false)

timeDifference(zdtFromString("2020-07-17 15:38:49+0000"), zdtFromString("2020-07-18 00:00:00+0000"))