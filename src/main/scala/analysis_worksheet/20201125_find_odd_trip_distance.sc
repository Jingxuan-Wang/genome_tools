// scala default libs

// other independency
import genome.util.Coordinate
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import toolkits.Utils
import toolkits.SparkCommon.writeCSV
import genome.util.Trajectory

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 25/11/20
* @project: genome-debug
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

val tripDF = spark.read.load("s3a://au-daas-compute/output/parquet/trip/{20201101,20201102,20201103,20201104}/*").withColumn("partition_date", split(input_file_name(), "/")(6))

tripDF.printSchema()

val dis_1101 = tripDF.where($"partition_date" === "20201101").groupBy("agentId").agg(sum("distance"))
val dis_1102 = tripDF.where($"partition_date" === "20201102").groupBy("agentId").agg(sum("distance").as("dis_1102"))
val dis_1103 = tripDF.where($"partition_date" === "20201103").groupBy("agentId").agg(sum("distance").as("dis_1103"))

val joint = dis_1102.join(dis_1103, Seq("agentId"), "inner").withColumn("diff", $"dis_1102" / $"dis_1103")
joint.show(10,false)
joint.describe("diff").show(false)

joint.count()

val odd_agents = joint.where($"diff" > 4).select("agentId")
val odd_tp_1102 = odd_agents.join(tripDF.where($"partition_date" === "20201102"), Seq("agentId"), "left")

val odd_tp_1103_com = odd_agents.join(tripDF.where($"partition_date" === "20201103"), Seq("agentId"), "left")

odd_tp_1102.repartition(1).write.mode(SaveMode.Overwrite).parquet("hdfs:///user/jingxuan/VAD/trip_1102")
odd_tp_1103_com.repartition(1).write.mode(SaveMode.Overwrite).parquet("hdfs:///user/jingxuan/VAD/trip_1103")


val odd_tp = spark.read.load("hdfs:///user/jingxuan/VAD/trip_1102")
val comp_tp = spark.read.load("hdfs:///user/jingxuan/VAD/trip_1103")

def numUpdates(tr: String):Int = {
  val trajectory = Trajectory.fromString(tr)
  trajectory.updates.length
}

val upUDF = udf[Int, String](numUpdates)

val odd_tp1 = odd_tp.withColumn("num_updates", upUDF($"trajectory"))
odd_tp.groupBy("dominantMode").count().show()

val odd_tp2 = odd_tp1.groupBy("dominantMode").agg(mean("num_updates"))
odd_tp2.show(10,false)

// loading data

// analysis

// saving result