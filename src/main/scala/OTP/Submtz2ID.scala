package OTP

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SparkSession}
import scopt.OptionParser

/**
  * This job is used to fix an issue existing in the original otp result. In the original output, we used submtz index as identity for each submtz, while in the geo hierarchy file in Singapore, the identity is id not the submtz index. To make it consistent, we will map the submtz index back to id in this job
  * @author jingxuan
  * @since 3/9/18
  *
  */
object Submtz2ID {
  val appName = this.getClass.getSimpleName

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Submtz2IDParams](appName) {
      opt[String]("input")
        .text("Input path for original otp result")
        .action((x, c) => c.copy(inputPath = x))
      opt[String]("id")
        .text("path for id list")
        .action((x, c) => c.copy(idPath = x))
      opt[String]("output")
        .text("Output folder for cleaned otp")
        .action((x, c) => c.copy(outputPath = x))
    }
    parser.parse(args, Submtz2IDParams()).map { params =>
      process(params)
    }
  }

  def process(params: Submtz2IDParams): Unit = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val inputDF =
      sc.textFile(params.inputPath).map(routeFromString).filter(x => x.isDefined).map(_.get).toDF()

    val forStart = spark.read
      .option("header", "true")
      .csv(params.idPath)
      .withColumnRenamed("sub_mtz", "startGeo")
      .withColumnRenamed("id", "startID")
    val forEnd = spark.read
      .option("header", "true")
      .csv(params.idPath)
      .withColumnRenamed("sub_mtz", "endGeo")
      .withColumnRenamed("id", "endID")

    val fixedDF = (inputDF
      .join(forStart, "startGeo"))
      .join(forEnd, "endGeo")
      .drop("startGeo")
      .drop("endGeo")
      .withColumnRenamed("startID", "startGeo")
      .withColumnRenamed("endID", "endGeo")
    fixedDF.map(routeToString(_)).write.text(params.outputPath)
    spark.stop()
  }

  def routeFromString(record: String): Option[Route] = {
    try {
      val fields = record.split("\t", -1)
      val dayOfWeek = fields(0)
      val queryTime = fields(1)
      val startGeo = fields(2).replace("\"", "")
      val endGeo = fields(3).replace("\"", "")
      val legs = fields(4).split(";", -1)
      val route = legs.map(routeLegFromString).toSeq
      Some(
        Route(
          dayOfWeek = dayOfWeek,
          queryTime = queryTime,
          startGeo = startGeo,
          endGeo = endGeo,
          route = route
        )
      )
    } catch {
      case e: Exception => None
    }
  }

  def routeLegFromString(record: String): RouteLeg = {
    val fields = record.split(",", -1)
    val startTime = fields(0)
    val startLon = fields(1).toDouble
    val startLat = fields(2).toDouble
    val endTime = fields(3)
    val endLon = fields(4).toDouble
    val endLat = fields(5).toDouble
    val mode = fields(6)
    val routeTaken = fields(7)
    RouteLeg(
      startTime = startTime,
      startLon = startLon,
      startLat = startLat,
      endTime = endTime,
      endLon = endLon,
      endLat = endLat,
      mode = mode,
      routeTaken = routeTaken
    )
  }

  def routeToString(row: Row): String = {
    val routeLeg = row.getAs[Seq[GenericRowWithSchema]]("route")
    val routeLegClass = routeLeg.map(row =>
      RouteLeg(
        startTime = row.getAs[String]("startTime"),
        startLon = row.getAs[Double]("startLon"),
        startLat = row.getAs[Double]("startLat"),
        endTime = row.getAs[String]("endTime"),
        endLon = row.getAs[Double]("endLon"),
        endLat = row.getAs[Double]("endLat"),
        mode = row.getAs[String]("mode"),
        routeTaken = row.getAs[String]("routeTaken")
      )
    )

    val route = Route(
      dayOfWeek = row.getAs[String]("dayOfWeek"),
      queryTime = row.getAs[String]("queryTime"),
      startGeo = row.getAs[String]("startGeo"),
      endGeo = row.getAs[String]("endGeo"),
      route = routeLegClass
    )
    val legStr = route.route
      .map(x =>
        Seq(
          x.startTime,
          x.startLon,
          x.startLat,
          x.endTime,
          x.endLon,
          x.endLat,
          x.mode,
          x.routeTaken
        ).mkString(",")
      )
      .mkString(";")
    Seq(route.dayOfWeek, route.queryTime, route.startGeo, route.endGeo, legStr).mkString("\t")
  }

  case class Submtz2IDParams(inputPath: String = "", idPath: String = "", outputPath: String = "")

  case class RouteLeg(
    startTime: String,
    startLon: Double,
    startLat: Double,
    endLon: Double,
    endLat: Double,
    endTime: String,
    mode: String,
    routeTaken: String
  )

  case class Route(
    dayOfWeek: String,
    queryTime: String,
    startGeo: String,
    endGeo: String,
    route: Seq[RouteLeg]
  )
}
