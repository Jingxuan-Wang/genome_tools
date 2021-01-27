// scala default libs

// other independency
import genome.util.{LBSRecord, Trajectory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import toolkits.SparkCommon.writeCSV
import toolkits.SqlUtils.isOverlap

// self defined libs

/**
* Problem Statement:
* Optus gonna switch lbs resource from poly to nio, for the trail round, we confront
* the issue that there is significant decrease after the switch testing. In this
* worknote, we are trying to find out if there is a way to make the number of results
* consistent by changing some cofigurations.
*
* Update: based on latest fix network has made until 30th Nov
*
* @author: jingxuan
* @date: 18/11/20
* @project: optus_source_switch
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

/**
  * basic config
  */

val base_path = "s3a://au-daas-compute/"
val nio_lbs = "input/lbs-transformed-niometrics/"
val poly_lbs = "input/lbs-transformed/"
val nio_tr = "output/raw/trajectory-niometrics/"
val poly_tr = "output/raw/trajectory/"
val nio_sp = "output/parquet/staypoint-niometrics/"
val poly_sp = "output/parquet/staypoint/"
val date1 = "20201125"
val date2 = "20201126"
val date3 = "20201127"

/**
  * step 1. compare the staypoint results to understand what is the reason of decrease
  *
  */

val w1 = Window.partitionBy("agent_id")
val w2 = Window.partitionBy("agent_id").orderBy("in_time")

/**
  * day 1
  */

val nio_sp_1125 = spark.read.load(base_path+nio_sp+date1).withColumn("nio_count", count("in_time").over(w1)).withColumn("nio_seq", row_number().over(w2))
val poly_sp_1125 = spark.read.load(base_path+poly_sp+date1).withColumn("poly_count", count("in_time").over(w1)).withColumn("poly_seq", row_number().over(w2))

val nio_num = nio_sp_1125.groupBy("agent_id").agg(count("in_time").as("nio_num"))
val poly_num = poly_sp_1125.groupBy("agent_id").agg(count("in_time").as("poly_num"))
val num_df_1125 = nio_num.join(poly_num, Seq("agent_id")).withColumn("nio-poly", $"nio_num" - $"poly_num")
num_df_1125.describe("nio-poly").show(false)
/**
  * +-------+-------------------+
  * |summary|nio-poly           |
  * +-------+-------------------+
  * |count  |5076287            |
  * |mean   |0.04371915929891277|
  * |stddev |1.5933307797322165 |
  * |min    |-22                |
  * |max    |22                 |
  * +-------+-------------------+
  */

num_df_1125.count() // 5076287
num_df_1125.where($"nio-poly" < 0).count() // 955851.0 / 5076287 = 0.1882972731841206
num_df_1125.where($"nio-poly" === 0).count() // 3120760.0 / 5076287  = 0.6147721750168972
num_df_1125.where($"nio-poly" > 0).count() // 999676.0 / 5076287 = 0.1969305517989822

val more_nio_sp_agent_1125 = num_df_1125.where($"nio-poly" > 0).select($"agent_id").distinct()
writeCSV(more_nio_sp_agent_1125, "hdfs:///user/jingxuan/VAD/NIO/20201125/more_nio_sp/agent_list")

/**
  * day2
  */

val nio_sp_1126 = spark.read.load(base_path+nio_sp+date2).withColumn("nio_count", count("in_time").over(w1)).withColumn("nio_seq", row_number().over(w2))
val poly_sp_1126 = spark.read.load(base_path+poly_sp+date2).withColumn("poly_count", count("in_time").over(w1)).withColumn("poly_seq", row_number().over(w2))

val nio_num_1126 = nio_sp_1126.groupBy("agent_id").agg(count("in_time").as("nio_num"))
val poly_num_1126 = poly_sp_1126.groupBy("agent_id").agg(count("in_time").as("poly_num"))
val num_df_1126 = nio_num_1126.join(poly_num_1126, Seq("agent_id")).withColumn("nio-poly", $"nio_num" - $"poly_num")
num_df_1126.describe("nio-poly").show(false)
/**
  * +-------+-------------------+
  * |summary|nio-poly           |
  * +-------+-------------------+
  * |count  |4991487            |
  * |mean   |0.10885914357785566|
  * |stddev |1.525169570300873  |
  * |min    |-22                |
  * |max    |28                 |
  * +-------+-------------------+
  */

num_df_1126.count() // 4991487
num_df_1126.where($"nio-poly" < 0).count() // 901436.0 / 4991487 = 0.18059468050302446
num_df_1126.where($"nio-poly" === 0).count() // 3051574.0 / 4991487  = 0.6113556942049534
num_df_1126.where($"nio-poly" > 0).count() // 1038477.0 / 4991487  = 0.2080496252920222

/**
  * day3
  */
val nio_sp_1127 = spark.read.load(base_path+nio_sp+date3).withColumn("nio_count", count("in_time").over(w1)).withColumn("nio_seq", row_number().over(w2))
val poly_sp_1127 = spark.read.load(base_path+poly_sp+date3).withColumn("poly_count", count("in_time").over(w1)).withColumn("poly_seq", row_number().over(w2))

val nio_num_1127 = nio_sp_1127.groupBy("agent_id").agg(count("in_time").as("nio_num"))
val poly_num_1127 = poly_sp_1127.groupBy("agent_id").agg(count("in_time").as("poly_num"))
val num_df_1127 = nio_num_1127.join(poly_num_1127, Seq("agent_id")).withColumn("nio-poly", $"nio_num" - $"poly_num")
num_df_1127.describe("nio-poly").show(false)
/**
 * +-------+-------------------+
 *|summary|nio-poly           |
 *+-------+-------------------+
 *|count  |5034857            |
 *|mean   |0.13292969393172438|
 *|stddev |1.5275111379434758 |
 *|min    |-19                |
 *|max    |26                 |
 *+-------+-------------------+
  */

num_df_1127.count() // 5034857
num_df_1127.where($"nio-poly" < 0).count() // 903683.0 / 5034857 = 0.17948533592910385
num_df_1127.where($"nio-poly" === 0).count() // 3054406.0 / 5034857  = 0.6066519863424125
num_df_1127.where($"nio-poly" > 0).count() // 1076768.0 / 5034857  = 0.21386267772848364

val less_nio_sp_agent_1127 = num_df_1127.where($"nio-poly" > 0).select($"agent_id").distinct()
writeCSV(less_nio_sp_agent_1127, "hdfs:///user/jingxuan/VAD/NIO/20201127/less_nio_sp/agent_list")

val more_nio_sp_agent_1127 = num_df_1127.where($"nio-poly" > 0).select($"agent_id").distinct()
writeCSV(more_nio_sp_agent_1127, "hdfs:///user/jingxuan/VAD/NIO/20201127/more_nio_sp/agent_list")


val spJoinCondition_1125 = isOverlap(nio_sp_1125("in_time"), nio_sp_1125("out_time"), poly_sp_1125("in_time"), poly_sp_1125("out_time")) && nio_sp_1125("agent_id") === poly_sp_1125("agent_id")

/**
  * join two stay point table by agent_id and two stay point has temporal overlap
  */
val join_sp_1125 = poly_sp_1125.join(nio_sp_1125, spJoinCondition_1125, "left_outer").where($"nio_count" < $"poly_count").select(poly_sp_1125("agent_id"),
        poly_sp_1125("in_time").as("in_time_poly"),
        poly_sp_1125("out_time").as("out_time_poly"),
        nio_sp_1125("in_time").as("in_time_nio"),
        nio_sp_1125("out_time").as("out_time_nio"),
        poly_sp_1125("geo_unit_id").as("geo_unit_id_poly"),
        nio_sp_1125("geo_unit_id").as("geo_unit_id_nio"),
        poly_sp_1125("sp_type").as("sp_type_poly"),
        nio_sp_1125("sp_type").as("sp_type_nio"),
        poly_sp_1125("status").as("status_poly"),
        nio_sp_1125("status").as("status_nio"),
        poly_sp_1125("cell_list").as("cell_list_poly"),
        nio_sp_1125("cell_list").as("cell_list_nio"),
        poly_sp_1125("radius").as("radius_poly"),
        nio_sp_1125("radius").as("radius_nio"),
        $"poly_count", $"nio_count", $"poly_seq", $"nio_seq")

join_sp_1125.show(20,false)

/**
  * 1.1 finding reasons that nio based has less staypoint
  */
val less_nio_sp_1125 = join_sp_1125.where($"poly_count" > $"nio_count").select("agent_id", "poly_count", "nio_count", "poly_seq", "nio_seq").groupBy("agent_id", "poly_count", "nio_count").agg(collect_list($"poly_seq").as("poly_seq_list"), collect_list($"nio_seq").as("nio_seq_list"))

less_nio_sp_1125.show(1, false)

less_nio_sp_1125.select("agent_id").distinct.count() // 54515

def uniqueItem(r: Seq[Int]): Int = {
        r.distinct.length
}
val uniqueUDF = udf[Int, Seq[Int]](uniqueItem)
/**
  * 1.1.1 we see everything only in poly
  * condition:
  * 1. any j in {m} can be mapped in {n}
  * 2. i in {n} but not find in {m}
  */
val condition1 = (uniqueUDF($"nio_seq_list") === $"nio_count") && (uniqueUDF($"poly_seq_list") < $"poly_count")
val case1_1125 = less_nio_sp_1125.where(condition1)
case1_1125.show(10,false)
case1_1125.count() // 29799 out of 54515
29799.0 / 54515 // 54.6%
writeCSV(case1_1125.select($"agent_id").distinct(), "hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/agent_list")

writeCSV(case1_1125.select($"agent_id").join(join_sp_1125, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/join_sp")
writeCSV(case1_1125.select($"agent_id").join(poly_sp_1125, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/poly_sp")
writeCSV(case1_1125.select($"agent_id").join(nio_sp_1125, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/nio_sp")

/**
  * 1.1.2 we see everything only in nio
  * condition:
  * 1. any i in {n} can be mapped in {m}
  * 2. j in {m} but not find in {n}
  */
val condition2 = (uniqueUDF($"poly_seq_list") === $"poly_count") && (uniqueUDF($"nio_seq_list") < $"nio_count")
val case2_1125 = less_nio_sp_1125.where(condition2)
case2_1125.show(10,false)
case2_1125.count() // 773 out of 54515
773.0 / 54515 // 1.41%

/**
  * 1.1.3 we see some in poly and some in nio
  * condition:
  * 1. i in {n} but not find in {m}
  * 2. j in {m} but not find in {n}
  */
val condition3 = (uniqueUDF($"poly_seq_list") < $"poly_count") && (uniqueUDF($"nio_seq_list") < $"nio_count")
val case3_1125 = less_nio_sp_1125.where(condition3)
case3_1125.show(10,false)
case3_1125.count() // 1980 out of 54515
1980.0 / 54515 // 3.63%

/**
  * 1.1.4 we see everything both in poly and in nio
  */

val condition4 = (uniqueUDF($"poly_seq_list") === $"poly_count") && (uniqueUDF($"nio_seq_list") === $"nio_count")
val case4_1125 = less_nio_sp_1125.where(condition4)
case4_1125.show(10,false)
case4_1125.count() // 21963 out of 54515
21963.0 / 54515 // 40.29%


writeCSV(case4_1125.select($"agent_id"), "hdfs:///user/jingxuan/VAD/NIO/20201125/sp_in_both/agent_list")
writeCSV(case4_1125.select($"agent_id").join(poly_sp_1125, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201125/sp_in_both/poly_sp")
writeCSV(case4_1125.select($"agent_id").join(nio_sp_1125, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201125/sp_in_both/nio_sp")
writeCSV(case4_1125.select($"agent_id").join(join_sp_1125, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201125/sp_in_both/join_sp")

/**
  * 2.1 exploring trajectory for scenario 1 and scenario 4
  */
val scenario1_agent_1125 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/agent_list").as[String].collect.toSet
val tr_nio_1125_select = sc.textFile(base_path+nio_tr).map(Trajectory.fromString(_)).filter(t => scenario1_agent_1125.contains(t.agentId))
val tr_poly_1125_select = sc.textFile(base_path+poly_tr).map(Trajectory.fromString(_)).filter(t => scenario1_agent_1125.contains(t.agentId))
tr_nio_1125_select.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/nio_tr")
tr_poly_1125_select.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/poly_tr")


/**
  * 3.1 exploring lbs for scenario 1 and scenario 4
  *
  */
//val scenario1_agent_1116 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/"+date1+"/unique_sp_in_poly/agent_list").as[String].collect.toSet
val lbs_nio_scenario1_1125 = sc.textFile(base_path+nio_lbs).map(i => LBSRecord.fromString(i, "Australia/Sydney")).flatMap(x => x).filter(t => scenario1_agent_1125.contains(t.agentID))
val lbs_poly_scenario1_1125 = sc.textFile(base_path+poly_lbs).map(i => LBSRecord.fromString(i, "Australia/Sydney")).flatMap(x => x).filter(t => scenario1_agent_1125.contains(t.agentID))
lbs_nio_scenario1_1125.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/nio_lbs")
lbs_poly_scenario1_1125.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/poly_lbs")
lbs_nio_scenario1_1125.take(1)


val lbs_nio_scenario1 = sc.textFile("hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/nio_lbs").map(i => LBSRecord.fromString(i, "UTC")).flatMap(x => x).map(l => (l.agentID, 1)).toDF("agentId", "n").groupBy("agentId").agg(count("n").as("num_record"))
val lbs_poly_scenario1 = sc.textFile("hdfs:///user/jingxuan/VAD/NIO/20201125/unique_sp_in_poly/poly_lbs").map(i => LBSRecord.fromString(i, "UTC")).flatMap(x => x).map(l => (l.agentID, 1)).toDF("agentId", "n").groupBy("agentId").agg(count("n").as("num_record"))
val joint_lbs = lbs_nio_scenario1.join(lbs_poly_scenario1, Seq("agentId")).withColumn("nio-poly", lbs_nio_scenario1("num_record") - lbs_poly_scenario1("num_record"))
joint_lbs.describe("nio-poly").show(false)


/**
  * 3.2 exploring all lbs that related to cases that sp in nio is less than in poly
  *
  */

/**
  * day1
  */
val less_nio_agent_1125 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/20201125/less_nio_sp/agent_list").as[String].collect.toSet
val more_nio_agent_1125 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/20201125/more_nio_sp/agent_list").as[String].collect.toSet
val lbs_nio_1125 = sc.textFile(base_path+nio_lbs+date1).map(i => LBSRecord.fromString(i, "Australia/Sydney")).flatMap(x => x).filter(t => more_nio_agent_1125.contains(t.agentID))
val lbs_poly_1125 = sc.textFile(base_path+poly_lbs+date1).map(i => LBSRecord.fromString(i, "Australia/Sydney")).flatMap(x => x).filter(t => more_nio_agent_1125.contains(t.agentID))

val nio_1125 = lbs_nio_1125.map(l => (l.agentID, 1)).toDF("agentId", "n").groupBy("agentId").agg(count("n").as("num_in_nio"))
val poly_1125 = lbs_poly_1125.map(l => (l.agentID, 1)).toDF("agentId", "n").groupBy("agentId").agg(count("n").as("num_in_poly"))
val lbs_num_df = nio_1125.join(poly_1125, Seq("agentId"))

lbs_num_df.withColumn("nio-poly", $"num_in_nio" - $"num_in_poly").describe("nio-poly").show(false)

/**
  * day3
  */
val less_nio_agent_1127 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/20201127/less_nio_sp/agent_list").as[String].collect.toSet
val more_nio_agent_1127 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/20201127/more_nio_sp/agent_list").as[String].collect.toSet
val lbs_nio_1127 = sc.textFile(base_path+nio_lbs+date1).map(i => LBSRecord.fromString(i, "Australia/Sydney")).flatMap(x => x).filter(t => more_nio_agent_1127.contains(t.agentID))
val lbs_poly_1127 = sc.textFile(base_path+poly_lbs+date1).map(i => LBSRecord.fromString(i, "Australia/Sydney")).flatMap(x => x).filter(t => more_nio_agent_1127.contains(t.agentID))

val nio_1127 = lbs_nio_1127.map(l => (l.agentID, 1)).toDF("agentId", "n").groupBy("agentId").agg(count("n").as("num_in_nio"))
val poly_1127 = lbs_poly_1127.map(l => (l.agentID, 1)).toDF("agentId", "n").groupBy("agentId").agg(count("n").as("num_in_poly"))
val lbs_num_df3 = nio_1127.join(poly_1127, Seq("agentId"))

lbs_num_df3.withColumn("nio-poly", $"num_in_nio" - $"num_in_poly").describe("nio-poly").show(false)


case class CellInventoryAUSchema(
                                  siteId: String,
                                  siteName: String,
                                  lac: String,
                                  lgaName: String,
                                  siteType: String,
                                  cellLon: Option[Double],
                                  cellLat: Option[Double],
                                  technology: String,
                                  band: String,
                                  cellSector: String,
                                  cellId: String,
                                  cellName: String,
                                  sacId: String,
                                  sa3Code: String,
                                  rncBsc: String,
                                  cellFirstSeen: String,
                                  antennaAzimuth: Option[Int],
                                  beamWidthHorizontal: Option[Int],
                                  beamWidthVertical: Option[Int],
                                  antennaElectricDownTilt: String,
                                  antennaMechanicalDownTilt: String,
                                  antennaHeight: String,
                                  antennaGain: String,
                                  antennaType: String,
                                  cellPower: String,
                                  feederLength: String,
                                  feederSize: String,
                                  cellStatus: String)