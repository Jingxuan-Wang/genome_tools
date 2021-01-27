// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.DataFrame

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 21/7/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
def checkExistence(path:String): Boolean = {
  val conf = sc.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)
  val exists = fs.exists(new org.apache.hadoop.fs.Path(path))
  exists
}

def readBatches(start: Long, end: Option[Long], inputPath:String, period:Int = 0, interval:Int = 300): Unit= {
  val numBatches =  if(end.isEmpty) period/interval else (end.get - start)/interval
  val batchInputPaths = (0L to numBatches - 1L).map(i => inputPath+(start+i*interval).toString+"000")
  batchInputPaths.map(i => (i, checkExistence(i))).filter(t => !t._2).map(t => t._1).foreach(println)
}

readBatches(1589990400, Some(1590249600), "hdfs:///user/jingxuan/RES/rtsp/lbs_sim/time=")
// analysis

// saving result