// scala default libs

// other independency
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

// self defined libs

/**
    * Description
    *
* @author: jingxuan
    * @date: 15/1/21
    * @project: bugfix
    *
*/

// Initializing spark
  val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

val vorpoly = sc.textFile("hdfs:///user/etl-cellmapping/voronoid/output/polygon_1.15/20201226")
val vorDF = vorpoly.map(r => (r.split("\\|")(0), r.split("\\|")(1))).toDF("cgi", "shape")
vorDF.show(3, false)

vorDF.count()
vorDF.where($"shape".contains("coordinates")).count()
vorDF.where(not($"shape".contains("coordinates"))).show(10,false)

val kdeImproved = sc.textFile("hdfs:///genome_v1_15/cell_mapping/cells_improved_by_kde/2019_04_06")
val kdeImprovedDF = kdeImproved.map(r => r.split(",")(2)).toDF("cgi")
//  525-1-315-33724, 525-1-315-33725, 525-1-315-33726
kdeImprovedDF.where($"cgi" === "525-1-315-33725").show(1)
// loading data
val sp1 = spark.read.load("hdfs:///genome_v1_15/parquet/staypoint/20201227")
val sp2 = spark.read.load("hdfs:///genome_v1_15/parquet/staypoint/20201228")
sp1.printSchema()
val target = "525-1-315-33726"
sp2.where($"cell_list".contains(target)).count()

val vorP = sc.textFile("hdfs:///user/etl-cellmapping/voronoid/preprocessed_1.15/20201226")
val vorDF = vorP.map(i => (i.split(",")(0), i.split(",")(1), i.split(",")(2), i.split(",")(3), i.split(",")(4), i.split(",")(5), i.split(",")(6))).toDF("cgi", "coord", "type", "lon", "lat", "azimuth", "beam_width")
vorDF.where($"cgi" === target).show(1,false)

val pre_cell = spark.read.csv("hdfs:///user/etl-cellmapping/voronoid/preprocessed_1.15/20201226")
val preDF = pre_cell.withColumnRenamed("_c0", "cgi").withColumnRenamed("_c1", "coords").withColumnRenamed("_c2", "type").withColumnRenamed("_c3", "lon").withColumnRenamed("_c4", "lat").withColumnRenamed("_c5","azimuth").withColumnRenamed("_c6", "beam_width")
preDF.printSchema()

preDF.where($"type" === "3G").select("lat", "lon").coalesce(1).write.mode(SaveMode.Overwrite).csv("hdfs:///user/jingxuan/VAD/voronoi_poly")

preDF.where($"cgi" === "525-1-315-33725").show(1)

val poly = sc.textFile("hdfs:///user/etl-cellmapping/voronoid/output/polygon/20201226")
poly.take(1)

// analysis

// saving result