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
* Update: in the latest data, network team in Optus fixed few issues in the probe.
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

val base_path = "s3a://au-daas-users/michael/niometrics_migration/"
val nio_lbs = "/location_events_to_lbs_transformed/nio_mascot/"
val poly_lbs = "/location_events_to_lbs_transformed/poly_mascot/"
val nio_tr = "/output/trajectory/nio_mascot/"
val poly_tr = "/output/trajectory/poly_mascot/"
val nio_sp = "/output/staypoint/nio_mascot/"
val poly_sp = "/output/staypoint/poly_mascot/"
val date1 = "20201116"
val date2 = "20201117"
val date3 = "20201118"

/**
  * step 1. compare the staypoint results to understand what is the reason of decrease
  *
  */

val w1 = Window.partitionBy("agent_id")
val w2 = Window.partitionBy("agent_id").orderBy("in_time")

val nio_sp_1116 = spark.read.load(base_path+nio_sp+date1).withColumn("nio_count", count("in_time").over(w1)).withColumn("nio_seq", row_number().over(w2))
val poly_sp_1116 = spark.read.load(base_path+poly_sp+date1).withColumn("poly_count", count("in_time").over(w1)).withColumn("poly_seq", row_number().over(w2))

val nio_num = nio_sp_1116.groupBy("agent_id").agg(count("in_time").as("nio_num"))
val poly_num = poly_sp_1116.groupBy("agent_id").agg(count("in_time").as("poly_num"))
val num_df_1116 = nio_num.join(poly_num, Seq("agent_id")).withColumn("nio-poly", $"nio_num" - $"poly_num")
num_df_1116.describe("nio-poly").show(false)
/**
  * +-------+--------------------+
  * |summary|nio-poly            |
  * +-------+--------------------+
  * |count  |263373              |
  * |mean   |-0.20247329832594838|
  * |stddev |1.2131432171343326  |
  * |min    |-20                 |
  * |max    |11                  |
  * +-------+--------------------+
  */
num_df_1116.count() // 263373
num_df_1116.where($"nio-poly" < 0).count() // 54774 out of 263373, 20.8%
num_df_1116.where($"nio-poly" === 0).count() // 176091 out of 263373, 66.8%
num_df_1116.where($"nio-poly" > 0).count() // 32508 out of 263373, 12.4%

val spJoinCondition_1116 = isOverlap(nio_sp_1116("in_time"), nio_sp_1116("out_time"), poly_sp_1116("in_time"), poly_sp_1116("out_time")) && nio_sp_1116("agent_id") === poly_sp_1116("agent_id")

/**
  * join two stay point table by agent_id and two stay point has temporal overlap
  */
val join_sp_1116 = poly_sp_1116.join(nio_sp_1116, spJoinCondition_1116, "left_outer").where($"nio_count" < $"poly_count").select(poly_sp_1116("agent_id"),
        poly_sp_1116("in_time").as("in_time_poly"),
        poly_sp_1116("out_time").as("out_time_poly"),
        nio_sp_1116("in_time").as("in_time_nio"),
        nio_sp_1116("out_time").as("out_time_nio"),
        poly_sp_1116("geo_unit_id").as("geo_unit_id_poly"),
        nio_sp_1116("geo_unit_id").as("geo_unit_id_nio"),
        poly_sp_1116("sp_type").as("sp_type_poly"),
        nio_sp_1116("sp_type").as("sp_type_nio"),
        poly_sp_1116("status").as("status_poly"),
        nio_sp_1116("status").as("status_nio"),
        poly_sp_1116("cell_list").as("cell_list_poly"),
        nio_sp_1116("cell_list").as("cell_list_nio"),
        poly_sp_1116("radius").as("radius_poly"),
        nio_sp_1116("radius").as("radius_nio"),
        $"poly_count", $"nio_count", $"poly_seq", $"nio_seq")

join_sp_1116.show(20,false)

/**
  * 1.1 finding reasons that nio based has less staypoint
  */
val less_nio_sp_1116 = join_sp_1116.where($"poly_count" > $"nio_count").select("agent_id", "poly_count", "nio_count", "poly_seq", "nio_seq").groupBy("agent_id", "poly_count", "nio_count").agg(collect_list($"poly_seq").as("poly_seq_list"), collect_list($"nio_seq").as("nio_seq_list"))

less_nio_sp_1116.show(1, false)

less_nio_sp_1116.select("agent_id").distinct.count() // 54515

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
val case1_1116 = less_nio_sp_1116.where(condition1)
case1_1116.show(10,false)
case1_1116.count() // 29799 out of 54515
29799.0 / 54515 // 54.6%
writeCSV(case1_1116.select($"agent_id"), "hdfs:///user/jingxuan/VAD/NIO/20201116/unique_sp_in_poly/agent_list")

writeCSV(case1_1116.select($"agent_id").join(join_sp_1116, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201116/unique_sp_in_poly/join_sp")
writeCSV(case1_1116.select($"agent_id").join(poly_sp_1116, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201116/unique_sp_in_poly/poly_sp")
writeCSV(case1_1116.select($"agent_id").join(nio_sp_1116, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201116/unique_sp_in_poly/nio_sp")

/**
  * 1.1.2 we see everything only in nio
  * condition:
  * 1. any i in {n} can be mapped in {m}
  * 2. j in {m} but not find in {n}
  */
val condition2 = (uniqueUDF($"poly_seq_list") === $"poly_count") && (uniqueUDF($"nio_seq_list") < $"nio_count")
val case2_1116 = less_nio_sp_1116.where(condition2)
case2_1116.show(10,false)
case2_1116.count() // 773 out of 54515
773.0 / 54515 // 1.41%

/**
  * 1.1.3 we see some in poly and some in nio
  * condition:
  * 1. i in {n} but not find in {m}
  * 2. j in {m} but not find in {n}
  */
val condition3 = (uniqueUDF($"poly_seq_list") < $"poly_count") && (uniqueUDF($"nio_seq_list") < $"nio_count")
val case3_1116 = less_nio_sp_1116.where(condition3)
case3_1116.show(10,false)
case3_1116.count() // 1980 out of 54515
1980.0 / 54515 // 3.63%

/**
  * 1.1.4 we see everything both in poly and in nio
  */

val condition4 = (uniqueUDF($"poly_seq_list") === $"poly_count") && (uniqueUDF($"nio_seq_list") === $"nio_count")
val case4_1116 = less_nio_sp_1116.where(condition4)
case4_1116.show(10,false)
case4_1116.count() // 21963 out of 54515
21963.0 / 54515 // 40.29%


writeCSV(case4_1116.select($"agent_id"), "hdfs:///user/jingxuan/VAD/NIO/20201116/sp_in_both/agent_list")
writeCSV(case4_1116.select($"agent_id").join(poly_sp_1116, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201116/sp_in_both/poly_sp")
writeCSV(case4_1116.select($"agent_id").join(nio_sp_1116, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201116/sp_in_both/nio_sp")
writeCSV(case4_1116.select($"agent_id").join(join_sp_1116, Seq("agent_id"), "left_outer"), "hdfs:///user/jingxuan/VAD/NIO/20201116/sp_in_both/join_sp")

/**
  * 2.1 exploring trajectory for scenario 1 and scenario 4
  */
val scenario1_agent_1116 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/"+date1+"/unique_sp_in_poly/agent_list").as[String].collect.toSet
val tr_nio_1116_select = sc.textFile(base_path+nio_tr+date1).map(Trajectory.fromString(_)).filter(t => scenario1_agent_1116.contains(t.agentId))
val tr_poly_1116_select = sc.textFile(base_path+poly_tr+date1).map(Trajectory.fromString(_)).filter(t => scenario1_agent_1116.contains(t.agentId))
tr_nio_1116_select.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201116/unique_sp_in_poly/nio_tr")
tr_poly_1116_select.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201116/unique_sp_in_poly/poly_tr")


/**
  * 3.1 exploring lbs for scenario 1 and scenario 4
  *
  */
//val scenario1_agent_1116 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/"+date1+"/unique_sp_in_poly/agent_list").as[String].collect.toSet
val lbs_nio_1116_select = sc.textFile(base_path+nio_lbs+date1).map(i => LBSRecord.fromString(i, "Australia/Sydney")).flatMap(x => x).filter(t => scenario1_agent_1116.contains(t.agentID))
val lbs_poly_1116_select = sc.textFile(base_path+poly_lbs+date1).map(i => LBSRecord.fromString(i, "Australia/Sydney")).flatMap(x => x).filter(t => scenario1_agent_1116.contains(t.agentID))
lbs_nio_1116_select.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201116/unique_sp_in_poly/nio_lbs")
lbs_poly_1116_select.map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/VAD/NIO/20201116/unique_sp_in_poly/poly_lbs")








val scenario4_agent_1025 = spark.read.csv("hdfs:///user/jingxuan/VAD/NIO/20201025/sp_in_both/agent_list").as[String].collect.toSet

spark.read.load("hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/join_sp").repartition(1).write.option("header", true).csv("hdfs:///user/jingxuan/VAD/NIO/20201025/unique_sp_in_poly/join_sp_csv")