// scala default libs

// other independency
import genome.util.Trajectory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import timeOp.TimeCommon._

// self defined libs

/**
    * Description
    *
* @author: jingxuan
    * @date: 13/5/20
    * @project: analysis
    *
*/

// Initializing spark
  val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
// loading homework
val hw02 = spark.read.load("s3a://au-daas-compute/bootstrapScripts/meta-input/homework/202002")


def getList(
      sdate: String,
      edate: String,
      path: String = "s3a://au-daas-archive/trajectory/"
  ): Dataset[Row] = {
    val dates = dateRange(sdate, edate, step = 1).map(x => x.replace("-", ""))
    val RDDs =
      dates.map(d => sc.textFile(s"$path$d").map(Trajectory.fromString).map(t => t.agentId))
    val agents = sc.union(RDDs.toSeq).toDF("agentId").distinct()
    agents
  }
// for Jan and Feb
val agent01 = getList("2020-01-01", "2020-01-31").withColumn("month", lit("Jan"))

val agent021 = getList("2020-02-01", "2020-02-10").withColumn("month", lit("Feb"))

val agent022 =
    getList("2020-02-11", "2020-02-29", path = "s3a://au-daas-compute/output/raw/trajectory/")
      .withColumn("month", lit("Feb"))
val agent02 = agent021.union(agent022)

// analysis

// get new agent that appears on Feb
val newAgentOnFeb = agent02.select($"agentId", $"month".as("month2")).join(agent01.select($"agentId", $"month".as("month1")), "agentId").where($"month2" === "Feb" && $"month1".isNull)

newAgentOnFeb.count()

val newAgentHW = newAgentOnFeb.select($"agentId".as("agent_id")).join(hw02, Seq("agent_id"), "left")
newAgentHW.where($"home".isNotNull).count

// For Dec 2019 and Jan 2020
val decDate1 = (dateRange("2019-12-09", "2019-12-16", 1).toSeq ++ dateRange("2019-12-18", "2019-12-19", 1) ++ dateRange("2019-12-21", "2019-12-31", 1).toSeq).map(d => d.replace("-", ""))
val decDate2 = dateRange("2019-12-01", "2019-12-08", 1).toSeq.map(d => d.replace("-", "")) ++ Seq("20191217", "20191220")


// saving result
