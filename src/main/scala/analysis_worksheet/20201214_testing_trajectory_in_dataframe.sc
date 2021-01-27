// scala default libs

// other independency
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import genome.util.DataPath

// self defined libs

/**
    * Description
    *
* @author: jingxuan
    * @date: 14/12/20
    * @project: Genome
    *
*/

// Initializing spark
  val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._


val test = Seq(("x", 1), ("x", 5), ("y", 2), ("y", 3)).toDF("a", "b")

case class T(a: String, b: Int)

def insertRow(rows: Iterator[Row]): Seq[T] = {
  val lis = rows.map(r=> (r.getAs[String]("a"), r.getAs[Int]("b"))).toList
  val first = lis.head
  val last = lis.last
  val id = first._1
  ((last._2 - first._2) > 1) match {
    case true => (first._2 to last._2 by 1).map(i => T(a=id, b=i))
    case _ => Seq[T]()
  }
}

test.repartition($"a").foreachPartition(insertRow(_))

val t = test.repartition($"a").foreachPartition()
// loading data

val lbs_path = DataPath.getPath(spark, "20201130", "lbs").get
val lbs = sc.textFile(lbs_path)
lbs.take(1)


val cellH1 = spark.read.load("s3a://au-daas-compute/output/parquet/cell_hierarchy/20201031")
cellH1.count()
val cellH2 = spark.read.load("s3a://au-daas-compute/output/parquet/cell_hierarchy/20201101")
cellH2.count()
cellH1.intersect(cellH2).count()
// analysis

// saving result