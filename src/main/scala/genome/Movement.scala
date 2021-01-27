package genome

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, explode, lit, udf}
import scopt.OptionParser
import toolkits.SparkCommon.arrayRepeat
import genome.util.Coordinate.haversine
import genome.util.Coordinate

/**
  *
  * @author jingxuan
  * @since 26/3/20
  *
  */

object Movement {

  val appName = this.getClass.getSimpleName

  // scalastyle:off
  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Params](appName) {
      opt[String]("input")
        .text("Input directory for input")
        .action((x, c) => c.copy(inputPath = x))

      opt[String]("hw")
        .text("Input directory for homework")
        .action((x, c) => c.copy(homeworkPath = x))

      opt[String]("geo")
        .text("Input directory for geo Hierarchy")
        .action((x, c) => c.copy(geoHierarchy = x))

      opt[String]("weight")
        .text("Input directory for weight table")
        .action((x, c) => c.copy(weightPath = x))

      opt[String]("date")
        .text("Input date")
        .action((x, c) => c.copy(date = x))

      opt[String]("output1")
        .text("output directory")
        .action((x, c) => c.copy(outputPath1 = x))

      opt[String]("output2")
        .text("output directory")
        .action((x, c) => c.copy(outputPath2 = x))
    }
    parser.parse(args, Params()).map { params =>
      process(params)
    }
  }
  // scalastyle:on

  def process(params: Params): Unit = {
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._

    val sp = spark.read.load(params.inputPath)
    val homework = spark.read
      .load(params.homeworkPath)
      .where(
        $"home" =!= "NA"
        && $"work" =!= "NA"
      )
      .select($"agent_id", $"home", $"work")

    val geoHierarchy = spark.read
      .load(params.geoHierarchy)
      .select($"geo_hierarchy_base_id".as("home"), $"sa1", $"sa2")

    val groupByAgent =
      sp.groupBy($"agent_id").agg(collect_list($"geo_unit_id").as("geo_units"))

    val joinWithHW = homework.join(groupByAgent, Seq("agent_id"), "inner")
    val joinWithGeo = joinWithHW.join(geoHierarchy, Seq("home"))
    val radiusUDF = udf[Double, Seq[String], String](radiusFromHome)
    val hwUDF = udf[Double, String, String](homeWorkDistance)

    val res =
      joinWithGeo
        .withColumn("max_dis", radiusUDF($"geo_units", $"home"))
        .withColumn("hw_dist", hwUDF($"home", $"work"))
        .where($"hw_dist" >= 1000)
        .withColumn("index", $"max_dis" / $"hw_dist")
        .withColumn("date", lit(params.date))
        .select("agent_id", "date", "home", "index", "sa1", "sa2")

    val weight = spark.read.csv(params.weightPath)

    // explode weight table based on given weight on each records
    val arrayRepeatUDF = udf[Array[String], Int, String](arrayRepeat)
    val duplicateWeight = weight
      .withColumn("agent_id", explode(arrayRepeatUDF($"_c1", $"_c0")))
      .select("agent_id")

    val resJoinWithWeight = duplicateWeight
      .join(res, Seq("agent_id"), "left")
      .where($"index".isNotNull) // remove records which doesn't have index

    resJoinWithWeight.registerTempTable("movement")

    // running sql query to get mean and median and save data to outputPath
    val sa1Query =
      "SELECT sa1, date, mean(index) as mean, percentile_approx(index, 0.5) as median FROM movement GROUP BY sa1, date"
    val sa2Query =
      "SELECT sa2, date, mean(index) as mean, percentile_approx(index, 0.5) as median FROM movement GROUP BY sa2, date"
    val sa1 = spark.sqlContext.sql(sa1Query)
    val sa2 = spark.sqlContext.sql(sa2Query)
    sa1.repartition(1).write.parquet(params.outputPath1)
    sa2.repartition(1).write.parquet(params.outputPath2)

    spark.stop()
  }

  def homeWorkDistance(home: String, work: String): Double = {
    val homeCoords = Coordinate(
      lon = home.split("_")(3).toDouble,
      lat = home.split("_")(4).toDouble
    )
    val workCoords = Coordinate(
      lon = work.split("_")(3).toDouble,
      lat = work.split("_")(4).toDouble
    )
    val distance = haversine(homeCoords, workCoords)
    distance
  }

  def radiusFromHome(geoUnits: Seq[String], target: String): Double = {
    val targetCoords = Coordinate(
      lon = target.split("_")(3).toDouble,
      lat = target.split("_")(4).toDouble
    )
    val coords =
      geoUnits.map(s => Coordinate(lon = s.split("_")(3).toDouble, lat = s.split("_")(4).toDouble))
    val maxDistance = coords.map(c => haversine(c, targetCoords)).max
    maxDistance
  }

  case class Params(
    inputPath: String = "",
    homeworkPath: String = "",
    geoHierarchy: String = "",
    weightPath: String = "",
    date: String = "",
    outputPath1: String = "",
    outputPath2: String = ""
  )

}
