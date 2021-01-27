import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import timeOp.TimeCommon.dateRange
import toolkits.SparkCommon.{arrayRepeat, hasColumn}


val spark = new SparkSession()

val sc = spark.sparkContext

import spark.implicits._

val dates = dateRange(from="2020-01-01",to="2020-03-31",step= 1).toList.map(d => d.replace("-", ""))

val arrayRepeatUDF = udf[Array[String], Int, String](arrayRepeat)

def processingData(date: String): Unit = {
  val sa1Query = "SELECT sa1, date, mean(index) as mean, percentile_approx(index, 0.5) as median FROM %s GROUP BY sa1, date"
  val sa2Query = "SELECT sa2, date, mean(index) as mean, percentile_approx(index, 0.5) as median FROM %s GROUP BY sa2, date"
  //reading weight table
  val weight = spark.read.csv("s3a://au-daas-compute/input/dailyweights/"+date)
  val duplicateWeight = weight.withColumn("agent_id", explode(arrayRepeatUDF($"_c1", $"_c0")))
    .select("agent_id")

  // reading movement input
  val inputPath = "s3a://au-daas-users/jingxuan/TEST/movement/"+date
  val input = spark.read.load(inputPath)

  // check if date column is present, if not, add in the date column with right value
  // if yes, just join with weight table and filter out agent_id without index
  val jointWithWeight = hasColumn(input, "date") match{
    case true => duplicateWeight.join(input, Seq("agent_id"), "left").where($"index".isNotNull)
    case _ => val inputModify = input.withColumn("date", lit(date))
      duplicateWeight.join(inputModify, Seq("agent_id"), "left").where($"index".isNotNull)
  }
  jointWithWeight.registerTempTable("mv_"+date)

  // running sql query and save data
  val sa1 = spark.sqlContext.sql(sa1Query.format("mv_"+date))
  val sa2 = spark.sqlContext.sql(sa2Query.format("mv_"+date))
  sa1.repartition(1).write.parquet("s3a://au-daas-users/jingxuan/TEST/movement/sa1/"+date)
  sa2.repartition(1).write.parquet("s3a://au-daas-users/jingxuan/TEST/movement/sa2/"+date)
}

dates.map(processingData(_))

