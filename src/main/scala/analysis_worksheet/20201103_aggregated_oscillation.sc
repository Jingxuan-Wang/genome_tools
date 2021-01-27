// scala default libs

// other independency
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 31/10/20
* @project: greenfield
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val df = spark.read.load("hdfs:///user/jingxuan/RES/tr_20201028_oscillation_on_grid")

df.printSchema()

// aggregate to sa1 and standardize by sa4
val sa1W = Window.partitionBy("sa1")
val sa4W = Window.partitionBy("sa4")
val df1 = df.withColumn("count", count($"cgi").over(sa1W)).withColumn("max", max($"count").over(sa4W)).withColumn("stand_count", $"count".as[Double]/ $"max".as[Double])
// analysis
df1.select("sa1","stand_count").distinct().repartition(1).write.mode(SaveMode.Overwrite).csv("hdfs:///user/jingxuan/RES/tr_oscillation_sa1_20201028")

// saving result

