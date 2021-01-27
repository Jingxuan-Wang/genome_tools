// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.dataspark.genome.computer.StayPointComputer
// self defined libs

/**
    * Description
    *
* @author: jingxuan
    * @date: 22/1/21
    * @project: presentation
    *
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext

val tr =
import spark.implicits._

// loading data

// analysis

// saving result