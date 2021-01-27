// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag



// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 16/10/20
* @project: globe
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

case class MRSchema(
                     agent_id: String,
                     hour: Int,
                     update_type: String,
                     site_name: String,
                     cell_type: String,
                     lon: Double,
                     lat: Double,
                     cell_tech: String,
                     cell_name: String
                   )

// loading data
def caseClassToSchema[A<: Product: TypeTag] = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]

val schema = caseClassToSchema[MRSchema]

val input = spark.read.option("delimiter", "\t").schema(schema).csv("hdfs:///user/jingxuan/TEST/bacolod_20_locations_20200518")

val cleaned = input.where($"lon".isNotNull)
val firstOrderGroup = cleaned.groupBy($"agent_id", $"hour").agg(collect_list(struct($"lat",
  $"lon", $"update_type", $"site_name", $"cell_type", $"cell_tech", $"cell_name")).alias("attrs"))

val secondOrderGroup = firstOrderGroup.groupBy("agent_id").agg(collect_list(struct($"hour", $"attrs")).as("records"))


def Test(res: Seq[GenericRowWithSchema]): Int = {
  val record = res.map(r => (r.getAs[Int]("hour"),
    r.getAs[Seq[GenericRowWithSchema]]("attrs").map(
      i =>
        (i.getAs[Double]("lat"),
          i.getAs[Double]("lon"),
          i.getAs[String]("update_type"),
          i.getAs[String]("site_name"),
          i.getAs[String]("cell_type"),
          i.getAs[String]("cell_tech"),
          i.getAs[String]("cell_name"))
    )))
  record.map(_._1).sum
}

val testUDF = udf[Int, Seq[GenericRowWithSchema]](Test)

val test = secondOrderGroup.withColumn("test", testUDF($"records")).select("test")
test.show(1,false)
// analysis

// saving result