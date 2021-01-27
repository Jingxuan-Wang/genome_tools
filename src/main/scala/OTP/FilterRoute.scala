//package OTP
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
//import scopt.OptionParser
//import org.apache.spark.sql.functions.udf
//
//import scala.collection.mutable
//
///**
//  *
//  * @author jingxuan
//  * @since 2019-11-06
//  *
//  */
//
//object FilterRoute{
//  case class Params(
//  inputPath: String = "",
//  geoHierarchyPath: String = "",
//  outputPath: String = ""
//  )
//
//  case class RouteCandidate(
//                           startTime: String,
//                           startLon: Double,
//                           startLat: Double,
//                           endLon: Double,
//                           endLat: Double,
//                           endTime: String,
//                           mode: String,
//                           routeTaken: String
//                           )
//  val target = Seq("TRAIN", "FERRY", "TRAM", "SUBWAY")
//
//  val appName = this.getClass.getSimpleName
//
//  def main(args: Array[String]): Unit = {
//    val parser = new OptionParser[Params](appName) {
//      opt[String]("input")
//        .text("Input directory for staypoint")
//        .action((x, c) => c.copy(inputPath = x))
//
//      opt[String]("geo")
//        .text("Input directory for Geo Hierarchy")
//        .action((x, c) => c.copy(geoHierarchyPath = x))
//
//      opt[String]("output")
//        .text("Input directory for output")
//        .action((x, c) => c.copy(outputPath = x))
//    }
//    parser.parse(args, Params()).map { params =>
//      process(params)
//    }
//  }
//
//  def process(params: Params): Unit = {
//    val spark = SparkSession.builder().appName(appName).getOrCreate()
//    import spark.implicits._
//
//  /*
//  Schema for route
//   |-- dayOfWeek: string (nullable = true)
//   |-- startSA1: string (nullable = true)
//   |-- endSA1: string (nullable = true)
//   |-- timeBandStart: array (nullable = true)
//   |    |-- element: string (containsNull = true)
//   |-- timeBandEnd: array (nullable = true)
//   |    |-- element: string (containsNull = true)
//   |-- candidate: array (nullable = true)
//   |    |-- element: struct (containsNull = true)
//   |    |    |-- _1: string (nullable = true) start time
//   |    |    |-- _2: string (nullable = true) end time
//   |    |    |-- _3: string (nullable = true) polygon
//   |    |    |-- _4: array (nullable = true) routeTaken
//   |    |    |    |-- element: struct (containsNull = true)
//   |    |    |    |    |-- startTime: string (nullable = true)
//   |    |    |    |    |-- startLon: double (nullable = true)
//   |    |    |    |    |-- startLat: double (nullable = true)
//   |    |    |    |    |-- endLon: double (nullable = true)
//   |    |    |    |    |-- endLat: double (nullable = true)
//   |    |    |    |    |-- endTime: string (nullable = true)
//   |    |    |    |    |-- mode: string (nullable = true)
//   |    |    |    |    |-- routeTaken: string (nullable = true)
//   |    |    |-- _5: double (nullable = true) distance
//
//   */
//    val route = spark.read.load(params.inputPath)
//
//    val geo = spark.read.load(params.geoHierarchyPath).select("sa1", "state").withColumnRenamed("sa1", "startSA1").distinct
//
//    val joint = route.join(geo, Seq("startSA1"))
//
//    val isPublicTransportUDF = udf[Boolean, mutable.WrappedArray[GenericRowWithSchema]](isPublicTransport)
//
//    val nsw = joint.where($"state" === "1").where(isPublicTransportUDF($"candidate"))
//
//    val getRouteUDF = udf[String, mutable.WrappedArray[GenericRowWithSchema]](getRoute)
//
//    val res = nsw.withColumn("public_link_id", getRouteUDF($"candidate"))
//
//    res.select("public_link_id").write.parquet(params.outputPath)
//
//    spark.stop()
//  }
//
//  def isPublicTransport(row: mutable.WrappedArray[GenericRowWithSchema]): Boolean = {
//    val modes = row.map(r=> r.getString(4).asInstanceOf[RouteCandidate]).map(r => target.contains
//    (r.mode)).reduce(_ && _)
//    return modes
//  }
//
//  def getRoute(row: mutable.WrappedArray[GenericRowWithSchema]): String = {
//    val data = row.map(r => r.get(4))
//    val publicRoute = data.filter(r => target.contains(r.mode)).map(_.routeTaken)
//    publicRoute.mkString(";")
//  }
//
//}
