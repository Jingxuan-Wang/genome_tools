// scala default libs

// other independency
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import toolkits.SqlUtils.gridToCoord
// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 25/9/20
* @project: DeepStayPoint
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

val initDate = "20191101"

val numTrainDays = 30

val numTestDays = 14

val sampleFraction = 0.15    //0.00001 relates to ~20 users (very_small_sample)  // 0.001  - 2053 users (small_sample) // 0.1 - 205,381 users (big_sample) // 0.01 - 20,538 users (medium_sample)

val time_interval = 15   // (time aggregation in intervals)

val save_path = "/user/jingxuan/DeepSP/medium_sample/"

val hw = spark.read.option("delimiter", "\t").csv("/genome/CAG/homework/201912")

val hw_for_join = hw.withColumnRenamed("_c0", "agent_id").withColumnRenamed("_c1", "home").withColumnRenamed("_c5", "work").select("agent_id", "home", "work")
//////////////////////////////////////////////////////////////////////////////////////////////////

// process dates into lists
val year0 = initDate.slice(0,4).toInt
val month0 = initDate.slice(4,6).toInt
val day0 = initDate.slice(6,8).toInt
val traindates = (0 to (numTrainDays-1)).map(i => LocalDate.of(year0,month0,day0).plusDays(i).format(DateTimeFormatter.ofPattern("yyyyMMdd")).toString).toList
val testdates = (numTrainDays to (numTrainDays + numTestDays - 1)).map(i => LocalDate.of(year0,month0,day0).plusDays(i).format(DateTimeFormatter.ofPattern("yyyyMMdd")).toString).toList
val dates = (0 to (numTrainDays + numTestDays - 1)).map(i => LocalDate.of(year0,month0,day0).plusDays(i).format(DateTimeFormatter.ofPattern("yyyyMMdd")).toString).toList


// for the defined period of time (dates) get list of users with staypoint information every day

val imsi_path="/user/jingxuan/DeepSP/agent_list/"
//dates.filter(_ != "20191104").map(d => {
//  val sp = spark.read.parquet("/genome/parquet/staypoint/"+d).filter($"status" === "Complete").select("agent_id").distinct()
//  sp.withColumn("count", lit(1)).write.parquet(imsi_path+d)
//})

val UserList = spark.read.load(imsi_path+"*").groupBy("agent_id").agg(sum("count").as("count"))
val UserListCol = UserList.where($"count" >= dates.length - 1).select("agent_id")


// get sample of users and collect as List[String]
val sample_users = UserListCol.sample(false, sampleFraction, seed=2020)
sample_users.count()

val sample_seq = sample_users.map(x => x.mkString).collectAsList.toArray.toSeq
val sample_list = sample_seq.map( _.toString).toList

// create or extend map: agent_id(imsi) to user_id(random id)
// test if usermap exists
val usermapPath = save_path + "usermap_DeepSP"
val conf = sc.hadoopConfiguration
val fs = org.apache.hadoop.fs.FileSystem.get(conf)
val exists = fs.exists(new org.apache.hadoop.fs.Path(usermapPath))
// if usermap exists -> add new users, else -> create from scratch
if(exists){
  //load usermap
  val usermapPath = save_path + "usermap_DeepSP"
  val usermaploadDF = spark.read.parquet(usermapPath)
  val usermap_load = usermaploadDF.select("key","value").as[(String,String)].collect.toMap
  //compare which users in userslist are not in usermap_load.keys
  val userstoadd = sample_list.toSet diff usermap_load.keys.toSet
  val m = usermap_load.size + 1
  val n = userstoadd.size
  val newids = (m to (n + m - 1)).toArray.map(x => "u_" + x.toString)
  val usermap = usermap_load ++ (userstoadd zip newids).toMap
  //save
  usermap.toSeq.toDF("key","value").write.format("parquet").mode("overwrite").save(usermapPath)
  usermap.toSeq.toDF("key","value").write.format("csv").mode("overwrite").save(usermapPath + ".csv")
} else{
  // create usermap from scratch
  val n = sample_list.size
  val newids = (1 to n).toArray.map(x => "u_" + x.toString)
  val usermap = (sample_list zip newids).toMap
  //save
  val usermapPath = save_path + "usermap_DeepSP"
  usermap.toSeq.toDF("key","value").write.format("parquet").mode("overwrite").save(usermapPath)
  usermap.toSeq.toDF("key","value").write.format("csv").mode("overwrite").save(usermapPath + ".csv")
}
// load updated usermap
val usermapPath = save_path + "usermap_DeepSP"
val usermaploadDF = spark.read.parquet(usermapPath)
val usermap = usermaploadDF.select("key","value").as[(String,String)].collect.toMap

// read data for training, validation and testing
val traindates_splits = traindates.grouped(4).toList

val w = Window.partitionBy($"agent_id").orderBy($"in_time")
def mapNewID(imsi: String): String = usermap(imsi)
val mapNewID_UDF = udf(mapNewID _)

traindates_splits.zipWithIndex.map(i => {
  val dates = i._1
  val group_seq = i._2
  val sp_train = dates.filter(d => d != "20191104").map(d => spark.read.parquet("/genome/parquet/staypoint/" ++ d).withColumn("group_seq", lit(group_seq))).reduce(_.union(_)).select("agent_id","geo_unit_id","in_time","out_time","status", "group_seq").filter($"status" === "Complete").filter($"agent_id".isin(sample_seq:_*)).orderBy("agent_id","in_time")

  val sp_train_with_hw = sp_train.join(hw_for_join, Seq("agent_id"), "left")
  val sp_train_seq = sp_train_with_hw.withColumn("seq", row_number.over(w))
  val sp_train_round = sp_train_seq.withColumn("rin_time",  window(to_timestamp($"in_time"),time_interval.toString+" minutes")("start"))
  val sp_train_tindex = sp_train_round.withColumn("time_index", (date_format($"rin_time","u") - 1)*96 + (hour($"rin_time"))*4 + minute($"rin_time")/time_interval + 1).as("Int")
  val sp_train_time = sp_train_tindex.withColumn("dow", date_format($"in_time", "u")).withColumn("hod", hour($"in_time"))
  val sp_train_pro = sp_train_time.withColumn("y", substring($"geo_unit_id",29,10).cast(DecimalType(10, 6))).withColumn("x", substring($"geo_unit_id",40,8).cast(DecimalType(10, 6))).withColumn("home_x", gridToCoord($"home", "lat")).withColumn("home_y", gridToCoord($"home", "lon")).withColumn("work_x", gridToCoord($"work", "lat")).withColumn("work_y", gridToCoord($"work", "lon")).drop("geo_unit_id", "home", "work")
  val sample_train = sp_train_pro.select("agent_id", "group_seq", "seq", "time_index", "dow", "hod", "x", "y", "home_x", "home_y", "work_x", "work_y")
  sample_train.write.mode("append").csv(save_path + "sample_train")
})


// consolidate records for training set and put the right sequence number after concat
val sample_train_sep = spark.read.csv(save_path + "sample_train")
sample_train_sep.printSchema()

val w2 = Window.partitionBy("_c0").orderBy($"_c1".cast(IntegerType), $"_c2".cast(IntegerType))
val sample_train_sep_last = sample_train_sep.withColumn("correct_seq", row_number().over(w2))

sample_train_sep_last.select("_c0", "correct_seq", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11").write.mode("overwrite").csv(save_path + "sample_train_f")


val testpaths = testdates.map(day => "/genome/parquet/staypoint/" ++ day)
val sp_test = testdates.map(d => spark.read.parquet("/genome/parquet/staypoint/" ++ d)).reduce(_.union(_)).select("agent_id","geo_unit_id","in_time","out_time","status").filter($"status" === "Complete").filter($"agent_id".isin(sample_seq:_*)).orderBy("agent_id","in_time")

val sp_test_with_hw = sp_test.join(hw_for_join, Seq("agent_id"), "left")
val sp_test_seq = sp_test_with_hw.withColumn("seq", row_number.over(w))
val sp_test_round = sp_test_seq.withColumn("rin_time",  window(to_timestamp($"in_time"),time_interval.toString+" minutes")("start"))
val sp_test_tindex = sp_test_round.withColumn("time_index", (date_format($"rin_time","u") - 1)*96 + (hour($"rin_time"))*4 + minute($"rin_time")/time_interval + 1).as("Int")
val sp_test_time = sp_test_tindex.withColumn("dow", date_format($"in_time", "u")).withColumn("hod", hour($"in_time"))
val sp_test_pro = sp_test_time.withColumn("y", substring($"geo_unit_id",29,10).cast(DecimalType(10, 6))).withColumn("x", substring($"geo_unit_id",40,8).cast(DecimalType(10, 6))).withColumn("home_x", gridToCoord($"home", "lat")).withColumn("home_y", gridToCoord($"home", "lon")).withColumn("work_x", gridToCoord($"work", "lat")).withColumn("work_y", gridToCoord($"work", "lon")).drop("geo_unit_id", "home", "work")

val sample_test = sp_test_pro.select("agent_id","seq", "time_index", "dow", "hod", "x", "y", "home_x", "home_y", "work_x", "work_y")
sample_test.write.mode("overwrite").csv(save_path + "sample_test")
// create column to keep track of the sequence order

