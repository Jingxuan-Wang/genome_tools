// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import genome.util.Trajectory

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 26/10/20
* @project: staypoint
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
//val tr = sc.textFile("hdfs:///genome_v1_15/raw/trajectory/20201013")
//val trInput = tr.map(Trajectory.fromString(_))
//
//val df = trInput.map(t => (t.agentId, t.updates.length)).toDF("agent_id", "num_update")
//df.show(3)
//df.sort("num_update").show(5,false)
//df.filter($"num_update" >= 5 && $"num_update" <= 150).count() // 348,223 updates
//df.sort(desc("num_update")).show(5, false)  // around 300,000 updates
//
//trInput.take(1)
//
//trInput.filter(t => (t.agentId == "525016CFD64C8820D0F72E74B23CC82BDDF5A2") || (t.agentId == "525016E69F9AEB4E99FE724C5AC69C9B50699F") || (t.agentId == "525016B8C4C4E453E36F7D9CEB06DC46245D03") || (t.agentId == "52501699B5FCCAFED955FF35B6AA10EDF95D56") || (t.agentId == "525016CF9D5077A207BB9AC8BAE02008DACDF8")) .repartition(1).saveAsTextFile("hdfs:///user/jingxuan/TEST/tr_for_sp_performance")
//
//trInput.filter(t => (t.updates.length >= 5) && (t.updates.length <= 150)).map(_.toString).repartition(1).saveAsTextFile("hdfs:///user/jingxuan/TEST/tr_for_sp_performance_2")
// analysis

// saving result

/**
  * |agent_id                              |num_update|distinct cell|
  * |:------------------------------------:|:--------:|
  * |525016CFD64C8820D0F72E74B23CC82BDDF5A2|77006     |
  * |525016E69F9AEB4E99FE724C5AC69C9B50699F|66508     |
  * |525016B8C4C4E453E36F7D9CEB06DC46245D03|60963     |
  * |52501699B5FCCAFED955FF35B6AA10EDF95D56|52250     |
  * |525016CF9D5077A207BB9AC8BAE02008DACDF8|47436     |
  */

val test = sc.textFile("hdfs://ds-pro-head-02.dataspark.net:8020/genome_v1_15/raw/trajectory/20201013")
val df1 = test.map(Trajectory.fromString(_)).map(t => (t.agentId, t.updates.length)).toDF("agent_id", "total_updates")

val df2 = test.map(Trajectory.fromString(_)).map(t => t.updates.map(i => (t.agentId, i.cell))).flatMap(x=>x).toDF("agent_id", "cell").distinct().groupBy("agent_id").agg(count("cell").as("unique_cells"))
df1.join(df2, Seq("agent_id")).repartition(1).write.csv("hdfs:///user/jingxuan/TEST/tr_20201013_explore")

test.filter(Trajectory.fromString(_).agentId == "52501687C30ACA0AA452C430125E4ED8B14E05").repartition(1).saveAsTextFile("hdfs:///user/jingxuan/TEST/sample_tr2_20201013")

val df3 = test.map(Trajectory.fromString(_)).filter(u => u.agentId == "502161680593BCA371212B442FD798139B4E5E").map(u => u.updates.map(i => (u.agentId, i.lat, i.lon, i.timestamp, i.cell))).flatMap(x=>x).toDF("agent_id", "lat", "lon", "timestamp", "cell")








val tr_path = "hdfs:///genome_v1_15/raw/trajectory/"
val pr_tr_path = "hdfs:///genome_v1_15/intermediate/staypoint/staypoint_incomplete/"

val date = 20201028
val pre_date = date - 1




val tr = sc.textFile(tr_path+date.toString).map(Trajectory.fromString(_)).map(t => (t.agentId, t.updates.length)).toDF("agent_id", "num")

val pr_tr = sc.textFile(pr_tr_path+pre_date.toString).map(Trajectory.fromString(_)).map(t => (t.agentId, t.updates.length)).toDF("agent_id", "num")

tr.describe("num").show(false)
pr_tr.describe("num").show(false)
//
//tr.where($"total_updates" >= 10000).count()
//
//tr.where($"total_updates" === 482270).show(1,false)

val pre_tr = sc.textFile(pr_tr_path+pre_date.toString).filter(i => {
  val data = Trajectory.fromString(i)
  val len = data.updates.length
  len <= 6000
}).saveAsTextFile("hdfs:///user/jingxuan/TEST/pre_trajectory/20201027")

