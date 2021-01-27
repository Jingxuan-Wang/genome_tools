// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// self defined libs

/**
    * Description
    *
* @author: jingxuan
    * @date: 18/12/20
    * @project: trajectory
    *
*/

// Initializing spark
  val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

val w1 = Window.partitionBy($"id").orderBy("type")
val w2 = Window.partitionBy($"id")

val df = Seq(("a", 1, "out", "y", 2), ("a", 2, "in", "", 1), ("a", 3, "in", "", 1), ("b", 3, "out", "y", 2), ("b", 4, "out", "", 1)).toDF("id", "lat", "type", "kde", "weight")

val test = df.withColumn("weight_lat", $"lat" * $"weight").withColumn("has_indoor", sum(when($"type" === "in" ,1).otherwise(0)).over(w2)).withColumn("new_lat", when($"has_indoor" >= 1, first("lat").over(w1)).otherwise(sum($"weight_lat").over(w2)/ sum($"weight").over(w2)))

test.show(false)



// loading data

// analysis

// saving result