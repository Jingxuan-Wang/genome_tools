// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import genome.util.LBSRecord.fromString
import org.apache.spark.sql.expressions.Window
import toolkits.SqlUtils.isOverlap
import genome.util.Trajectory
import toolkits.SparkCommon.writeCSV

// self defined libs

/**
* Problem Statement:
* Optus gonna switch lbs resource from poly to nio, for the trail round, we confront
* the issue that there is significant decrease after the switch testing. In this
* worknote, we are trying to find out if there is a way to make the number of results
* consistent by changing some cofigurations.
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

val base_path = "/user/richard/niotests/"
val nio_lbs = "repartitionedlbsinput_nio_1000_imsis/"
val poly_lbs = "repartitionedlbsinput_poly_1000_imsis/"
val nio_tr = "trajectory_nio_1000_imsis/"
val poly_tr = "trajectory_poly_1000_imsis/"
val nio_sp = "staypoint_nio_1000_imsis/"
val poly_sp = "staypoint_poly_1000_imsis/"
val date1 = "20201025"
val date2 = "20201026"

/**
  * step 1. compare the staypoint results to understand what is the reason of decrease
  *
  */

val w1 = Window.partitionBy("agent_id")
val w2 = Window.partitionBy("agent_id").orderBy("in_time")

val nio_sp_1025 = spark.read.load(base_path+nio_sp+date1).withColumn("nio_count", count("in_time").over(w1)).withColumn("nio_seq", row_number().over(w2))
val poly_sp_1025 = spark.read.load(base_path+poly_sp+date1).withColumn("poly_count", count("in_time").over(w1)).withColumn("poly_seq", row_number().over(w2))

val nio_num = nio_sp_1025.groupBy("agent_id").agg(count("in_time").as("nio_num"))
val poly_num = poly_sp_1025.groupBy("agent_id").agg(count("in_time").as("poly_num"))
val num_df_1025 = nio_num.join(poly_num, Seq("agent_id")).withColumn("nio-poly", $"nio_num" - $"poly_num")
num_df_1025.describe("nio-poly").show(false)

/**
  * +-------+-------------------+
  * |summary|nio-poly           |
  * +-------+-------------------+
  * |count  |997                |
  * |mean   |-0.2778335005015045|
  * |stddev |1.7613772793019808 |
  * |min    |-9                 |
  * |max    |8                  |
  * +-------+-------------------+
  *
  */

num_df_1025.count() // 997
num_df_1025.where($"nio-poly" < 0).count() // 350 out of 997, 35%
num_df_1025.where($"nio-poly" === 0).count() // 420 out of 997, 42%
num_df_1025.where($"nio-poly" > 0).count() // 227 out of 997, 23%

val spJoinCondition_1025 = isOverlap(nio_sp_1025("in_time"), nio_sp_1025("out_time"), poly_sp_1025("in_time"), poly_sp_1025("out_time")) && nio_sp_1025("agent_id") === poly_sp_1025("agent_id")

/**
  * join two stay point table by agent_id and two stay point has temporal overlap
  */
val join_sp_1025 = poly_sp_1025.join(nio_sp_1025, spJoinCondition_1025, "left_outer").where($"nio_count" < $"poly_count").select(poly_sp_1025("agent_id"),
        poly_sp_1025("in_time").as("in_time_poly"),
        poly_sp_1025("out_time").as("out_time_poly"),
        nio_sp_1025("in_time").as("in_time_nio"),
        nio_sp_1025("out_time").as("out_time_nio"),
        poly_sp_1025("geo_unit_id").as("geo_unit_id_poly"),
        nio_sp_1025("geo_unit_id").as("geo_unit_id_nio"),
        poly_sp_1025("sp_type").as("sp_type_poly"),
        nio_sp_1025("sp_type").as("sp_type_nio"),
        poly_sp_1025("status").as("status_poly"),
        nio_sp_1025("status").as("status_nio"), $"poly_count", $"nio_count", $"poly_seq", $"nio_seq")

join_sp_1025.show(20,false)

/**
  * 1.1 finding reasons that nio based has less staypoint
  */
val less_nio_sp_1025 = join_sp_1025.where($"poly_count" > $"nio_count").select("agent_id", "poly_count", "nio_count", "poly_seq", "nio_seq").groupBy("agent_id", "poly_count", "nio_count").agg(collect_list($"poly_seq").as("poly_seq_list"), collect_list($"nio_seq").as("nio_seq_list"))

less_nio_sp_1025.show(1, false)

less_nio_sp_1025.select("agent_id").distinct.count() // 350

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
val case1_1025 = less_nio_sp_1025.where(condition1)
case1_1025.show(10,false)
case1_1025.count() // 134 out of 350
134.0 / 350 // 38.3%
// example = 2E1727BBF8A8B56F4DF20408079FB0E041FEFFEB3D0DCE66FE19C4432E115B2D
case1_1025.select($"agent_id").write.csv("hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/agent_list")

writeCSV(case1_1025.select($"agent_id").join(join_sp_1025, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/join_sp")
writeCSV(case1_1025.select($"agent_id").join(poly_sp_1025, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/poly_sp")

/**
  * 1.1.2 we see everything only in nio
  * condition:
  * 1. any i in {n} can be mapped in {m}
  * 2. j in {m} but not find in {n}
  */
val condition2 = (uniqueUDF($"poly_seq_list") === $"poly_count") && (uniqueUDF($"nio_seq_list") < $"nio_count")
val case2_1025 = less_nio_sp_1025.where(condition2)
case2_1025.show(10,false)
case2_1025.count() // 10 out of 350
10.0 / 350 // 2.85%
//example = 879A4A92D309DFE3B2F798085A1AD63DCA4EC61F6FFE24AA8B3216F8AEEEA78A

/**
  * 1.1.3 we see some in poly and some in nio
  * condition:
  * 1. i in {n} but not find in {m}
  * 2. j in {m} but not find in {n}
  */
val condition3 = (uniqueUDF($"poly_seq_list") < $"poly_count") && (uniqueUDF($"nio_seq_list") < $"nio_count")
val case3_1025 = less_nio_sp_1025.where(condition3)
case3_1025.show(10,false)
case3_1025.count() // 20 out of 350
20.0 / 350 // 5.71%
// example = B78227FB5480BA3859AB79BE9EF23E676D299901B87A23BED03F1E70EE59E805

/**
  * 1.1.4 we see everything both in poly and in nio
  */

val condition4 = (uniqueUDF($"poly_seq_list") === $"poly_count") && (uniqueUDF($"nio_seq_list") === $"nio_count")
val case4_1025 = less_nio_sp_1025.where(condition4)
case4_1025.show(10,false)
case4_1025.count() // 186 out of 350
186.0 / 350 // 53.14%
// example = B12981B868F1CDD7E9C8D2F06C327497EF3DB16859E4867561C1E9A55121B06D

case4_1025.select($"agent_id").write.csv("hdfs:///user/jingxuan/VAD/NIO/20201025/sp_in_both/agent_list")
writeCSV(case4_1025.select($"agent_id").join(poly_sp_1025, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201025/sp_in_both/poly_sp")
writeCSV(case4_1025.select($"agent_id").join(nio_sp_1025, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201025/sp_in_both/nio_sp")
writeCSV(case4_1025.select($"agent_id").join(join_sp_1025, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201025/sp_in_both/join_sp")

/**
  * 2.1 exploring trajectory for scenario 1 and scenario 4
  */
val scenario1_agent_1025 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/agent_list").as[String].collect.toSet
val tr_nio_1025_select = sc.textFile(base_path+nio_tr+date1).map(Trajectory.fromString(_)).filter(t => scenario1_agent_1025.contains(t.agentId))
val tr_poly_1025_select = sc.textFile(base_path+poly_tr+date1).map(Trajectory.fromString(_)).filter(t => scenario1_agent_1025.contains(t.agentId))
tr_nio_1025_select.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/nio_tr")
tr_poly_1025_select.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/poly_tr")










val scenario4_agent_1025 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/20201025/sp_in_both/agent_list").as[String].collect.toSet

spark.read.load("hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/join_sp").repartition(1).write.option("header", true).csv("hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/join_sp_csv")