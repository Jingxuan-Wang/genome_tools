// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source
import com.dataspark.genome.computer.StayPointComputer
import com.dataspark.genome.StayPoint.newGridComputer
import com.dataspark.util.Utils.trajectoryFromString
import java.time.{ZoneId,ZonedDateTime}
import com.dataspark.geogrid.grids.ISEA3H
import genome.util.Trajectory

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 26/11/20
* @project: NIO
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data

// analysis

// saving result