// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.dataspark.genome.GenomeSchema.CellInventoryAUSchema
import toolkits.SparkCommon.caseClassToSchema
import com.dataspark.genome.MasterCellGenerator.getFullCellIdUDF

// self defined libs

/**
    * Description
    *
* @author: jingxuan
    * @date: 15/12/20
    * @project: network
    *
*/

// Initializing spark
  val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val schema = caseClassToSchema[CellInventoryAUSchema]
val base_path = "s3a://au-daas-compute/input/cell_inventory_adjusted/"


val cell0 = spark.read.schema(schema).option("delimiter", "|").csv(base_path+"20200930")
val cell1 = spark.read.schema(schema).option("delimiter", "|").csv(base_path+"20201101")
val cell2 = spark.read.schema(schema).option("delimiter", "|").csv(base_path+"20201102")
val cell3 = spark.read.schema(schema).option("delimiter", "|").csv(base_path+"20201103")
val cell4 = spark.read.schema(schema).option("delimiter", "|").csv(base_path+"20201130")

val cell11 = cell1.withColumn("cgi", getFullCellIdUDF("AU")($"technology", $"lac", $"cellId", $"sacId", lit(""), lit(""))).select($"cgi", $"cellLon".as("lon"), $"cellLat".as("lat"))

val cell22 = cell2.withColumn("cgi", getFullCellIdUDF("AU")($"technology", $"lac", $"cellId", $"sacId", lit(""), lit(""))).select($"cgi", $"cellLon".as("lon"), $"cellLat".as("lat"))

val cell33 = cell3.withColumn("cgi", getFullCellIdUDF("AU")($"technology", $"lac", $"cellId", $"sacId", lit(""), lit(""))).select($"cgi", $"cellLon".as("lon"), $"cellLat".as("lat"))

val cell44 = cell4.withColumn("cgi", getFullCellIdUDF("AU")($"technology", $"lac", $"cellId", $"sacId", lit(""), lit(""))).select($"cgi", $"cellLon".as("lon"), $"cellLat".as("lat"))

cell11.except(cell22).count() // 6

cell22.except(cell33).count() // 28

cell44.except(cell11).count() // 19077

cell44.except(cell11).show(10,false)

val hi1 = spark.read.load("s3a://au-daas-compute/output/parquet/cell_hierarchy/20201031")
val hi2 = spark.read.load("s3a://au-daas-compute/output/parquet/cell_hierarchy/20201101")
val hi3 = spark.read.load("s3a://au-daas-compute/output/parquet/cell_hierarchy/20201115")
val hi4 = spark.read.load("s3a://au-daas-compute/output/parquet/cell_hierarchy/20201130")
hi1.count()
hi2.count()
hi3.count()

cell11.where($"cgi" === "505-02-53261-21792577").show(false)
cell44.where($"cgi" === "505-02-53261-21792577").show(false)
hi3.where($"cgi" === "505-02-53330-37892652").show(3,false)
hi4.where($"cgi" === "505-02-53261-21792577").show(false)
// analysis

val t = "505-02-37266-20785"
hi1.where($"cgi" === t).show(false)
hi2.where($"cgi" === t).show(false)
hi3.where($"cgi" === t).show(false)
hi4.where($"cgi" === t).show(false)

/**
  * check if total number of cgi remains same
  */
cell11.select("cgi").distinct().count() // 167606
cell22.select("cgi").distinct().count() // 167620
cell33.select("cgi").distinct().count() // 167634
cell44.select("cgi").distinct().count() // 168810

cell44.select("cgi").except(cell11.select("cgi")).show(10, false)
// val t2 = "505-02-23281-1958051857"
val t2 = "505-02-27504-1984483346"
hi4.where($"cgi" === t2).show(false)


/**
  * debug the local result
  */
val cell0930 = spark.read.schema(schema).option("delimiter", "|").csv(base_path+"20200930")
val cell1001 = spark.read.schema(schema).option("delimiter", "|").csv(base_path+"20201001")

val cgi0930 = cell0930.withColumn("cgi", getFullCellIdUDF("AU")($"technology", $"lac", $"cellId", $"sacId", lit(""), lit(""))).select($"cgi", $"cellLon".as("lon"), $"cellLat".as("lat"))

val cgi1001 = cell1001.withColumn("cgi", getFullCellIdUDF("AU")($"technology", $"lac", $"cellId", $"sacId", lit(""), lit(""))).select($"cgi", $"cellLon".as("lon"), $"cellLat".as("lat"))
val diff = cgi1001.select("cgi").except(cgi0930.select("cgi")).collect().map(r => r.getAs[String]("cgi"))

val hi_self = spark.read.load("hdfs:///user/jingxuan/TEST/cell_hierarchy/20200930")
val hi_self2 = spark.read.load("hdfs:///user/jingxuan/TEST/cell_hierarchy/20201001")
val t1 = "505-02-52508-20167947"
diff.length
cgi0930.where($"cgi".isin(diff: _*)).count()
cgi1001.where($"cgi".isin(diff: _*)).count()
hi_self2.where($"cgi".isin(diff: _*)).count()
// saving result

