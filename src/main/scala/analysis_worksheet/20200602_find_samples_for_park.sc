// scala default libs

// other independency
import genome.util.Trajectory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 2/6/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val sample = spark.read.load("hdfs:///user/changhong/realtimeSG/safedist_validation/rtsp_obs_in_btnr_15may_16may")

val target_agents = sample.select($"agentId").collect().map(r=>r.getAs[String](0)).toSet

val tr = sc.textFile("hdfs://ds-pro-head-02.dataspark.net:8020/genome/trajectory/20200515")

val selected_tr = tr.map(Trajectory.fromString(_)).filter(t => target_agents.contains(t.agentId)).map(_.toString)

selected_tr.repartition(1).saveAsTextFile("hdfs:///user/jingxuan/TEST/rtsp/tr_for_park")

// analysis

// saving result