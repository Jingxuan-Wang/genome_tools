// scala default libs

// other independency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// self defined libs

/**
* Description
*
* @author: jingxuan
* @date: 17/8/20
* @project: rtsp
*
*/

// Initializing spark
val spark = new SparkSession()

val sc = spark.sparkContext
import spark.implicits._

// loading data
val df_part1 = (20200727 to 20200731 by 1).map(i => spark.read.load("/user/jingxuan/TEST/rtsp/ambient/cell_based_2/v2/"+i.toString).withColumn("date", lit(i))).reduce(_.union(_))
val df_part2 = (20200801 to 20200814 by 1).map(i => spark.read.load("/user/jingxuan/TEST/rtsp/ambient/cell_based_2/v2/"+i.toString).withColumn("date", lit(i))).reduce(_.union(_))
val df = df_part1.union(df_part2)

val df2_part1 = (20200727 to 20200731 by 1).map(i => spark.read.load("/user/jingxuan/TEST/rtsp/ambient/imsi_11pm_4am/v2/"+i.toString).withColumn("date", lit(i))).reduce(_.union(_))
val df2_part2 = (20200801 to 20200816 by 1).map(i => spark.read.load("/user/jingxuan/TEST/rtsp/ambient/imsi_11pm_4am/v2/"+i.toString).withColumn("date", lit(i))).reduce(_.union(_))
val df2 = df2_part1.union(df2_part2)


val res = df.groupBy("poi_name", "date").agg(count("agentId"))
res.repartition(1).write.csv("hdfs:///user/jingxuan/TEST/cell_based_parks")

val res2 = df2.groupBy("poi_name", "date").agg(count("agentId"))

res2.repartition(1).write.csv("hdfs:///user/jingxuan/TEST/grid_based_parks")
// analysis

df.where($"poi_name".contains("Pasir Ris Park")).show(10,false)

// saving result