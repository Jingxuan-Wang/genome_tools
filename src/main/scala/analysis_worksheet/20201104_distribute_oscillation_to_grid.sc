// scala default libs

// other independency
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.dataspark.util.GridMapping._
import com.dataspark.geogrid.grids.ISEA3H
import genome.util.Coordinate
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

val resolution = 22
val grid = new ISEA3H(resolution)

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 30/10/20
* @project: greenfield
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val oscillation = spark.read.load("hdfs:///user/jingxuan/RES/tr_20201028_oscillation")
val geo_hierarchy = spark.read.load("s3a://au-daas-compute/output/parquet/aggregated-geo-hierarchy/20201028")
geo_hierarchy.printSchema()

def to_grid(r: GenericRowWithSchema): String = {
  val lat = r.getAs[Double]("lat")
  val lon = r.getAs[Double]("lon")
  val resolution = 22
  val grid = new ISEA3H(resolution)
  getGridFromCoordinate(lat, lon, grid, "EPSG:4326")
}

val gridUDF = udf[String, GenericRowWithSchema](to_grid)

val osDF = oscillation.withColumn("geo_hierarchy_base_id", gridUDF($"coord")).drop("coords", "bearing_degree")
osDF.join(geo_hierarchy.select("geo_hierarchy_base_id", "sa1", "sa2", "sa3", "sa4"), Seq("geo_hierarchy_base_id")).write.mode(SaveMode.Overwrite).parquet("hdfs:///user/jingxuan/RES/tr_20201028_oscillation_on_grid")

// analysis

// saving result