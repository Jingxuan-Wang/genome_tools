package cell

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.formatMapper.WktReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import scopt.OptionParser
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._

/**
  * This job is used to map combined cells to a set of sa1 which are intersected with the
  * boundary covered by the combined cells. To boost the performance, we also introduce SA3 and
  * SA2 into the computing process for brief search.
  *
  * @output: cell_list | [{"geoUnit": String, "percentage": Double}]
  * @author jingxuan
  * @since 2019-07-12
  *
  */

object MapCellCombinationToGeoLocation {
  private final val GEOJSON_FILE_DELIMITER = "\t"
  private final val CELL_TO_POLYGON_FILE_DELIMITER = "|"

  implicit object AnyJsonFormat extends JsonFormat[Any] {

    def write(x: Any) =
      x match {
        case n: Int                   => JsNumber(n)
        case n: Double                => JsNumber(n)
        case s: String                => JsString(s)
        case b: Boolean if b == true  => JsTrue
        case b: Boolean if b == false => JsFalse
      }

    def read(value: JsValue) =
      value match {
        case JsNumber(n) => n.intValue()
        case JsNumber(n) => n.doubleValue()
        case JsString(s) => s
        case JsTrue      => true
        case JsFalse     => false
      }
  }
  val appName = this.getClass.getSimpleName

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Params](appName) {

      opt[String]("topLevelGeoJsonFile")
        .text("Input directory for geoJson file")
        .action((x, c) => c.copy(topLevelGeoJsonInputPath = x))

      opt[String]("topLevelGeoJsonId")
        .text("Input directory for geoJson file")
        .action((x, c) => c.copy(topLevelGeoJsonIdKey = x))

      opt[String]("bottomLevelGeoJsonFile")
        .text("Input directory for geoJson file")
        .action((x, c) => c.copy(bottomLevelGeoJsonInputPath = x))

      opt[String]("bottomLevelGeoJsonId")
        .text("Input directory for geoJson file")
        .action((x, c) => c.copy(bottomLevelGeoJsonIdKey = x))

      opt[String]("intersecPolygon")
        .text("Input directory for cell to intersection polygon file ")
        .action((x, c) => c.copy(cellToIntersectionPolygonPath = x))

      opt[String]("outputPath")
        .text("Input for geoLocation ID key")
        .action((x, c) => c.copy(outputPath = x))
    }
    parser.parse(args, Params()).map { params =>
      process(params)

    }
  }

  def process(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kyro.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)

    val sc = spark.sparkContext

    // column index starting from 0
    val wktColumnInCell = 1
    val wktColumnInTopGeoJson = 2
    val wktColumnInBottomGeoJson = 3
    val allowTopologyInvalidGeometris = false // Optional
    val skipSyntaxInvalidGeometries = false // Optional

    // reading in cell Polygon file into SRDD and partition by QuadTree
    val cellRDD = WktReader.readToGeometryRDD(
      sc,
      params.cellToIntersectionPolygonPath,
      wktColumnInCell,
      allowTopologyInvalidGeometris,
      skipSyntaxInvalidGeometries
    )

    // read in top level geoJson file
    val topGeoJsonRDD = WktReader.readToGeometryRDD(
      sc,
      params.topLevelGeoJsonInputPath,
      wktColumnInTopGeoJson,
      allowTopologyInvalidGeometris,
      skipSyntaxInvalidGeometries
    )

    // read in bottom level geoJson file
    val bottomGeoJsonRDD = WktReader.readToGeometryRDD(
      sc,
      params.bottomLevelGeoJsonInputPath,
      wktColumnInBottomGeoJson,
      allowTopologyInvalidGeometris,
      skipSyntaxInvalidGeometries
    )

    cellRDD.analyze()
    cellRDD.spatialPartitioning(GridType.RTREE)
    bottomGeoJsonRDD.spatialPartitioning(cellRDD.getPartitioner)

    val considerBoundaryIntersection =
      false // Only return gemeotries fully covered by each query window in queryWindowRDD
    val usingIndex = false

    val joinWithBottomGeoJson = JoinQuery.SpatialJoinQuery(
      bottomGeoJsonRDD,
      cellRDD,
      usingIndex,
      considerBoundaryIntersection
    )

    val result = joinWithBottomGeoJson
      .groupByKey()
      .rdd
      .map(x => toDistributionJson(x._1, x._2.asScala.flatten(y => y.asScala)))

    result.saveAsTextFile(params.outputPath)

    spark.stop()

  }

  def toDistributionJson(cellPolygon: Geometry, geoShapes: Iterable[Geometry]): String = {
    val cells = cellPolygon.getUserData.asInstanceOf[String]
    val cell_coverage = geoShapes.map(g =>
      Map(
        "geoUnit"    -> g.getUserData.asInstanceOf[String],
        "percentage" -> toCoveragePercentage(g, cellPolygon)
      )
    )
    val cell_coverage_Json = cell_coverage.map(_.toJson).mkString(",")
    cells + "|" + "[" + cell_coverage_Json + "]"
  }

  def toCoveragePercentage(sa1: Geometry, cellPolygon: Geometry): Double = {
    val intersection = sa1.intersection(cellPolygon)
    val percentage = intersection.getArea / cellPolygon.getArea
    percentage
  }

  case class Params(
    topLevelGeoJsonInputPath: String = "",
    topLevelGeoJsonIdKey: String = "",
    bottomLevelGeoJsonInputPath: String = "",
    bottomLevelGeoJsonIdKey: String = "",
    cellToIntersectionPolygonPath: String = "",
    geoHierarchyInputPath: String = "",
    geoHierarchyColumnName: String = "",
    outputPath: String = ""
  )
}
