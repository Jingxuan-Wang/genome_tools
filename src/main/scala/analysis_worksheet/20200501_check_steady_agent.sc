// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import genome.util.Trajectory

// self defined libs

/**
* Trying to locate imsi that is steady but generate tons of records
*
* @author: jingxuan
* @date: 1/5/20
* @project: analysis
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val sp = spark.read.load("s3a://au-daas-archive/staypoint/20200101")
sp.printSchema()

// analysis
val trans = sp.withColumn("duration",unix_timestamp($"out_time") - unix_timestamp($"in_time"))
val grop = trans.groupBy("agent_id", "duration", "cell_list").count()
grop.show(1)

val suspect_agent = grop.withColumn("cell_count", size(split($"cell_list", ":"))).where(($"count"===1) && ($"duration" > 168000) && ($"cell_count" === 1))
suspect_agent.select("agent_id")

val tr = sc.textFile("s3a://au-daas-archive/trajectory/20200101")
tr.map(Trajectory.fromString).take(1)

// saving result