// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import timeOp.TimeCommon._
import toolkits.SparkBin.customizeBin
import genome.util.Coordinate


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

val sg_prod = "ds-pro-head-02.dataspark.net:8020"

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
val sim = readBatches(1595001600, Some(1595088000),"hdfs:///user/yasara/rstay/output_jul_17_18_new3/result_").withColumn("timeDiff", to_timestamp($"reportingTime").cast(LongType) - to_timestamp($"lastObserveTime").cast(LongType))

val prod = readBatches(1595001900, Some(1595088000), "hdfs:///user/jingxuan/TEST/rtsp_v2_debug/result_").withColumn("timeDiff", to_timestamp($"reportingTime").cast(LongType) - to_timestamp($"lastObserveTime").cast(LongType))

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
val durCond = $"timeDiff" <= 14400
val spCond = ($"curIsStayPoint" === "Yes") || ($"prevIsStayPoint" === "Yes")

val cleanSim = sim.where(durCond && spCond) // 874864065
val cleanProd = prod.where(durCond && spCond) // 1086134690


val simStay =
  cleanSim.withColumn("geoUnitId", when($"curIsStayPoint" === "Yes", $"curGeoUnitId").otherwise($"prevGeoUnitid")).withColumn("radius", when($"curIsStayPoint" === "Yes", $"curRadius").otherwise($"prevRadius")).withColumn("observeTime", when($"curIsStayPoint" === "Yes", $"lastObserveTime").otherwise($"prevLastObserveTime")).withColumn("duration", when($"curIsStayPoint" === "Yes", $"curDuration").otherwise($"prevDuration")).withColumn("cellList", when($"curIsStayPoint" === "Yes", $"curCellList").otherwise($"prevCellList")).select("agentId", "reportingTime", "observeTime", "geoUnitId", "radius", "duration", "cellList")

val prodStay =
  cleanProd.withColumn("geoUnitId", when($"curIsStayPoint" === "Yes", $"curGeoUnitId").otherwise($"prevGeoUnitid")).withColumn("radius", when($"curIsStayPoint" === "Yes", $"curRadius").otherwise($"prevRadius")).withColumn("observeTime", when($"curIsStayPoint" === "Yes", $"lastObserveTime").otherwise($"prevLastObserveTime")).withColumn("duration", when($"curIsStayPoint" === "Yes", $"curDuration").otherwise($"prevDuration")).withColumn("cellList", when($"curIsStayPoint" === "Yes", $"curCellList").otherwise($"prevCellList")).select("agentId", "reportingTime", "observeTime", "geoUnitId", "radius", "duration", "cellList")

simStay.write.parquet("hdfs:///user/jingxuan/VAD/rtsp_v2_sim_sp")

prodStay.show(3,false)

prodStay.write.parquet("hdfs:///user/jingxuan/VAD/rtsp_v2_prod_sp")

simStay.count() // 651692794

prodStay.count() // 602148617
// 8.23% more in simulation than in production.

/**
  * More detalied analysis on staypoint bwtween simulation and production result
  */

val simSP = spark.read.load("hdfs:///user/jingxuan/VAD/rtsp_v2_sim_sp")
val prodSP = spark.read.load("hdfs://"+sg_prod+"/user/jingxuan/VAD/rtsp_v2_prod_sp")

val simCount = simSP.groupBy("agentId").count()
simCount.write.parquet("hdfs:///user/jingxuan/VAD/rtsp_v2_sim_count")
val prodCount = prodSP.groupBy("agentId").count()
prodCount.write.parquet("hdfs:///user/jingxuan/VAD/rtsp_v2_prod_count")


val joinSP = simSP.select($"agentId", $"reportingTime", $"observeTime".as("observeTimeSim"), $"geoUnitId".as("geoUnitIdSim"), $"radius".as("radiusSim"), $"duration".as("durationSim"), $"cellList".as("cellListSim")).join(prodSP.select($"agentId", $"reportingTime", $"observeTime".as("observeTimeProd"), $"geoUnitId".as("geoUnitIdProd"), $"radius".as("radiusProd"), $"duration".as("durationProd"), $"cellList".as("cellListProd")), Seq("agentId", "reportingTime"))

joinSP.show(1,false)

joinSP.write.parquet("hdfs:///user/jingxuan/VAD/rtsp_v2_join_sp")

val join = spark.read.load("hdfs:///user/jingxuan/VAD/rtsp_v2_join_sp")

// looking at some examples that exists in production but not in join
val target = "204043DAD525419DC7B0214DF11071A85005D1"
simSP.where($"agentId" === target).sort("reportingTime").show(50,false)
prodSP.where($"agentId" === target).sort("reportingTime").show(50,false)

def distance(grid1: String, grid2: String): Double = {
  val coord1 = Coordinate(lat = grid1.split("_")(4).toDouble, lon = grid1.split("_")(3).toDouble)
  val coord2 = Coordinate(lat = grid2.split("_")(4).toDouble, lon = grid2.split("_")(3).toDouble)
  Coordinate.haversine(coord1, coord2)
}

val distUDF = udf[Double, String, String](distance)

val res = join.withColumn("timeDiff", to_timestamp($"observeTimeSim").cast(LongType) - to_timestamp($"observeTimeProd").cast(LongType)).withColumn("gridDis", distUDF($"geoUnitIdSim", $"geoUnitIdProd"))

res.show(10,false)

res.select("timeDiff").describe().show()

val simCount1 = spark.read.parquet("hdfs:///user/jingxuan/VAD/rtsp_v2_sim_count")
val prodCount1 = spark.read.parquet("hdfs:///user/jingxuan/VAD/rtsp_v2_prod_count")
val resCount = simCount1.withColumnRenamed("count", "simCount").join(prodCount1.withColumnRenamed("count", "prodCount"), Seq("agentId"))

val resBin = resCount.withColumn("diff", $"simCount" - $"prodCount")

val bin = customizeBin(Array(-250.0, -200.0, -150.0, -100.0, -50.0, 0.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0), "diff", "diffBin", resBin)
bin.groupBy("diffBin").count().sort("diffBin").show(50,false)