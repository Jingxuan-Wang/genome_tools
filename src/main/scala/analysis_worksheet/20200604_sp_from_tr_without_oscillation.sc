// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 4/6/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val sp = spark.read.load("hdfs:///user/prod-testing/hj_temp_staypoint/experimental_sp_for_jx/20200515").filter($"status" === "Complete").withColumnRenamed("geo_unit_id", "geoUnitId")

val poiMapping = spark.read.option("header", true).csv("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/total_grids_hexagon.csv").withColumnRenamed("grid_id", "geoUnitId").withColumn("poi_name", initcap(col("poi_name")))
//val ambientImsis = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/imsi_3am/*").withColumnRenamed("agentId", "agent_id")
val ambientImsis = spark.read.load("hdfs:///user/changhong/realtimeSG/safedist_validation/sp_park_ambient_imsis_202005{15,16}").withColumnRenamed("agentId", "agent_id")
val xtrapWeights = spark.read.load("hdfs://ds-pro-head-02.dataspark.net:8020/data/prod-realtime/extrapolation/20200322").withColumnRenamed("IMSI", "agent_id")
// analysis
// udf for getting sequence from start to end hour
val sequence = udf((start_date: String, start_hour: Int, end_date: String, end_hour: Int) => {
  val result = if (start_date < end_date) { Map(start_date -> (start_hour to 23).toArray, end_date -> (0 to end_hour).toArray) }
  else { Map(start_date -> (start_hour to end_hour).toArray) }
  result
}: Map[String, Array[Int]])

// get selected POIs and aggregate
val processed = sp.join(poiMapping, Seq("geoUnitId"), "inner").
  withColumn("h_in", hour($"in_time")). // already converted to SGT
  withColumn("h_out", hour($"out_time")).
  withColumn("in_date", date_format($"in_time", "yyyy-MM-dd")).
  withColumn("out_date", date_format($"out_time", "yyyy-MM-dd")).
  select($"agent_id", $"poi_name", $"poi_category", explode(sequence($"in_date", $"h_in", $"out_date", $"h_out"))).
  withColumn("hour", explode($"value")).
  withColumn("day_name", date_format($"key", "E"))

val res = processed.join(xtrapWeights, Seq("agent_id"), "left").
  join(ambientImsis, Seq("agent_id", "poi_name"), "left").
  withColumn("cleaned_weight", when($"isAmbient15" === 1, 0.0).otherwise($"weight")).
  groupBy("poi_name", "poi_category", "day_name", "hour").
  agg(sum("weight") as "count", sum("cleaned_weight") as "ambient_removed_count")
// saving result
res.repartition(1).write.csv("TEST/rtsp/sp_testing_with_tr_without_oscillation_updated")