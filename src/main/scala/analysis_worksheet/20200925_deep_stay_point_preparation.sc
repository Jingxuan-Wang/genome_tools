// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.JavaConversions._
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

val sampleFraction = 0.5    //0.00001 relates to ~20 users (very_small_sample)  // 0.001  - 2053 users (small_sample) // 0.1 - 205,381 users (big_sample) // 0.01 - 20,538 users (medium_sample)

val time_interval = 15   // (time aggregation in intervals)

val save_path = "/user/jingxuan/DeepSP/medium_sample/"

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

val UserList = spark.read.load(imsi_path+"*").groupBy("agent_id").count()
val UserListCol = UserList.where($"count" >= dates.length - 1).select("agent_id")
UserListCol.count()


// get sample of users and collect as List[String]
val sample_users = UserListCol.sample(false, sampleFraction, seed=2020)
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

traindates_splits.map(i => {
  val trainpaths = i.filter(d => d != "20191104").map(day => "/genome/parquet/staypoint/" ++ day)
  val sp_train = spark.read.parquet(trainpaths:_*).select("agent_id","geo_unit_id","in_time","out_time","status").filter($"status" === "Complete").filter($"agent_id".isin(sample_seq:_*)).orderBy("agent_id","in_time")
  val sp_train_seq = sp_train.withColumn("seq", row_number.over(w))
  val sp_train_round = sp_train_seq.withColumn("rin_time",  window(to_timestamp($"in_time"),time_interval.toString+" minutes")("start"))
  val sp_train_tindex = sp_train_round.withColumn("time_index", (date_format($"rin_time","u") - 1)*96 + (hour($"rin_time"))*4 + minute($"rin_time")/time_interval + 1).as("Int")
  val sp_train_time = sp_train_tindex.withColumn("dow", date_format($"in_time", "u")).withColumn("hod", hour($"in_time"))
  val sp_train_nid = sp_train_time.withColumn("user_id", mapNewID_UDF($"agent_id")).drop("agent_id")
  val sp_train_pro = sp_train_nid.withColumn("y", substring($"geo_unit_id",29,10).cast(DecimalType(10, 6)) - 103).withColumn("x", substring($"geo_unit_id",40,8).cast(DecimalType(10, 6)) - 1).drop("geo_unit_id")
  val sp_train_struct = sp_train_pro.withColumn("stay", struct("seq","time_index", "dow", "hod", "x", "y"))
  val sp_train_map = sp_train_struct.groupBy("user_id").agg(sort_array(collect_list($"stay")).as("stays")).orderBy(substring_index($"user_id", "_", -1).cast(IntegerType))
  val sp_train_maps = sp_train_map.withColumn("num_stays", size($"stays"))
  val sample_train = sp_train_maps.filter(!($"num_stays" === 1))
  sample_train.write.format("json").mode("append").option("multiline","false").save(save_path + "sample_train.json")
})


val testpaths = testdates.map(day => "/genome/parquet/staypoint/" ++ day)
val sp_test = spark.read.parquet(testpaths:_*).select("agent_id","geo_unit_id","in_time","out_time","status").filter($"status" === "Complete").filter($"agent_id".isin(sample_seq:_*)).orderBy("agent_id","in_time")
val sp_test_seq = sp_test.withColumn("seq", row_number.over(w))
val sp_test_round = sp_test_seq.withColumn("rin_time",  window(to_timestamp($"in_time"),time_interval.toString+" minutes")("start"))
val sp_test_tindex = sp_test_round.withColumn("time_index", (date_format($"rin_time","u") - 1)*96 + (hour($"rin_time"))*4 + minute($"rin_time")/time_interval + 1).as("Int")
val sp_test_time = sp_test_tindex.withColumn("dow", date_format($"in_time", "u")).withColumn("hod", hour($"in_time"))
val sp_test_nid = sp_test_time.withColumn("user_id", mapNewID_UDF($"agent_id")).drop("agent_id")
val sp_test_pro = sp_test_nid.withColumn("y", substring($"geo_unit_id",29,10).cast(DecimalType(10, 6)) - 103).withColumn("x", substring($"geo_unit_id",40,8).cast(DecimalType(10, 6)) - 1).drop("geo_unit_id")
val sp_test_struct = sp_test_pro.withColumn("stay", struct("seq","time_index", "dow", "hod", "x", "y"))
val sp_test_map = sp_test_struct.groupBy("user_id").agg(sort_array(collect_list($"stay")).as("stays")).orderBy(substring_index($"user_id", "_", -1).cast(IntegerType))
val sp_test_maps = sp_test_map.withColumn("num_stays", size($"stays"))
val sample_test = sp_test_maps.filter(!($"num_stays" === 1))
sample_test.write.format("json").mode("overwrite").save(save_path + "sample_test.json")
// create column to keep track of the sequence order


/**
  * Testing
  */

val trainpaths = traindates_splits(0).map(day => "/genome/parquet/staypoint/" ++ day)
val sp_train = trainpaths.map(spark.read.parquet(_)).reduce(_.union(_))
//  .select("agent_id","geo_unit_id","in_time","out_time","status")
//  .filter($"status" === "Complete").filter($"agent_id".isin(sample_seq:_*)).orderBy("agent_id","in_time")
sp_train.printSchema()
val sp_train_seq = sp_train.withColumn("seq", row_number.over(w))
val sp_train_round = sp_train_seq.withColumn("rin_time",  window(to_timestamp($"in_time"),time_interval.toString+" minutes")("start"))
val sp_train_tindex = sp_train_round.withColumn("time_index", (date_format($"rin_time","u") - 1)*96 + (hour($"rin_time"))*4 + minute($"rin_time")/time_interval + 1).as("Int")
val sp_train_time = sp_train_tindex.withColumn("dow", date_format($"in_time", "u")).withColumn("hod", hour($"in_time"))
val sp_train_nid = sp_train_time.withColumn("user_id", mapNewID_UDF($"agent_id")).drop("agent_id")
val sp_train_pro = sp_train_nid.withColumn("y", substring($"geo_unit_id",29,10).cast(DecimalType(10, 6)) - 103).withColumn("x", substring($"geo_unit_id",40,8).cast(DecimalType(10, 6)) - 1).drop("geo_unit_id")
val sp_train_struct = sp_train_pro.withColumn("stay", struct("seq","time_index", "dow", "hod", "x", "y"))
val sp_train_map = sp_train_struct.groupBy("user_id").agg(sort_array(collect_list($"stay")).as("stays")).orderBy(substring_index($"user_id", "_", -1).cast(IntegerType))
val sp_train_maps = sp_train_map.withColumn("num_stays", size($"stays"))
val sample_train = sp_train_maps.filter(!($"num_stays" === 1))
sample_train.show(1)
sample_train.write.format("json").mode("overwrite").save(save_path + "sample_train.json")

/*
	transform from in_time to a weekly 15 min interval index (day_hour_min)
	Mapping function: Day_of_week * 96 + hour * 4 + int(minute / interval) + 1
	where, Day_of_week -> Monday: 0 , Tuesday: 1
	e.g. Monday 00:00 -> 1 , Monday 00:15 -> 2, ...
*/
// round to intervals
// apply mapping function

// create day of week,and hour features

// sanitize data
// apply usermap to update to new user ids

// update new geoids(long, lat -> y, x); y = long -103; x = lat - 1

// transform into dataframe: user_id, stays[seq, time_index, x, y], num_stays | filter out num_stays = 1 | save
// training set

// test set

// save usermap length
val maplengths = Seq(usermap.size).toDF("usermapsize")
maplengths.write.format("csv").mode("overwrite").save(save_path + "maplengths.csv")


