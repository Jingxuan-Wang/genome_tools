package genome

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, explode, lit, udf}
import scopt.OptionParser
import toolkits.SparkCommon.arrayRepeat
import genome.util.Coordinate
import genome.util.Coordinate.haversine

/**
  *
  * @author jingxuan
  * @since 26/3/20
  *
  */

object MovementIndex {

  val appName = this.getClass.getSimpleName

  //scalastyle:off
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

      opt[String]("sa2")
        .text("output directory for sa2 level")
        .action((x, c) => c.copy(sa2OutputPath = x))

      opt[String]("gcc")
        .text("output directory for gcc level")
        .action((x, c) => c.copy(gccOutputPath = x))

      opt[String]("state")
        .text("output directory for gcc level")
        .action((x, c) => c.copy(stateOutputPath = x))

      opt[String]("nation")
        .text("output directory for state level")
        .action((x, c) => c.copy(nationOutputPath = x))
    }
    parser.parse(args, Params()).map { params =>
      process(params)
    }
  }
  //scalastyle:on

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
      .select($"geo_hierarchy_base_id".as("home"), $"sa2", $"gcc", $"state")

    val groupByAgent = sp.groupBy($"agent_id").agg(collect_list($"geo_unit_id").as("geo_units"))

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
        .select("agent_id", "date", "home", "index", "sa2", "gcc", "state")

    val weight = spark.read.csv(params.weightPath)

    // explode weight table based on given weight on each records
    val arrayRepeatUDF = udf[Array[String], Int, String](arrayRepeat)
    val duplicateWeight = weight
      .withColumn("agent_id", explode(arrayRepeatUDF($"_c1", $"_c0")))
      .select("agent_id")

    val resJoinWithWeight = duplicateWeight
      .join(res, Seq("agent_id"), "left")
      .where($"index".isNotNull) // remove records which doesn't have index

    val count = resJoinWithWeight.count()

    resJoinWithWeight.registerTempTable("movement")

    // running sql query to get mean and median and save data to outputPath
    val sa2Query =
      "SELECT sa2, date, mean(index) as mean, percentile_approx(index, 0.5) as median, count(agent_id) as weighted_agent_num FROM movement GROUP BY sa2, date"
    val gccQuery =
      "SELECT gcc, date, mean(index) as mean, percentile_approx(index, 0.5) as median, count(agent_id) as weighted_agent_num FROM movement GROUP BY gcc, date"
    val stateQuery =
      "SELECT state, date, mean(index) as mean, percentile_approx(index, 0.5) as median, count(agent_id) as weighted_agent_num FROM movement GROUP BY state, date"
    val nationQuery =
      "SELECT date, mean(index) as mean, percentile_approx(index, 0.5) as median,count(agent_id) as weighted_agent_num FROM movement GROUP BY date"
    val sa2 = spark.sqlContext.sql(sa2Query)
    sa2.repartition(1).write.parquet(params.sa2OutputPath)
    val gcc = spark.sqlContext.sql(gccQuery)
    gcc.repartition(1).write.parquet(params.gccOutputPath)
    val state = spark.sqlContext.sql(stateQuery)
    state.repartition(1).write.parquet(params.stateOutputPath)
    val nation = spark.sqlContext.sql(nationQuery)
    nation.repartition(1).write.parquet(params.nationOutputPath)

    spark.stop()
  }

  def homeWorkDistance(home: String, work: String): Double = {
    val homeCoords =
      Coordinate(lon = home.split("_")(3).toDouble, lat = home.split("_")(4).toDouble)
    val workCoords =
      Coordinate(lon = work.split("_")(3).toDouble, lat = work.split("_")(4).toDouble)
    val distance = haversine(homeCoords, workCoords)
    distance
  }

  def radiusFromHome(geoUnits: Seq[String], target: String): Double = {
    val targetCoords =
      Coordinate(lon = target.split("_")(3).toDouble, lat = target.split("_")(4).toDouble)
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
    sa2OutputPath: String = "",
    gccOutputPath: String = "",
    stateOutputPath: String = "",
    nationOutputPath: String = ""
  )

}
