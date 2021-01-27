package geojson

import com.esri.core.geometry.{
  Geometry,
  OperatorExportToWkt,
  OperatorImportFromJson,
  WktExportFlags
}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * This job is used to convert esri format to proper wkt format
  *
  * @author jingxuan
  * @since 2019-07-17
  *
  */

object Convert2Wkt {

  val appName = this.getClass.getSimpleName

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Params](appName) {
      opt[String]("input")
        .text("Input directory")
        .action((x, c) => c.copy(inputPath = x))

      opt[String]("output")
        .text("Output directory")
        .action((x, c) => c.copy(outputPath = x))
    }
    parser.parse(args, Params()).map { params =>
      process(params)
    }
  }

  def process(params: Params): Unit = {
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    val data =
      sc.textFile(params.inputPath).map(x => x.split("\\|")).filter(x => x(1).contains("rings"))
    val output = data.map(x => (x(0), toWKT(x(1)))).map(x => x._1 + "|" + x._2)
    output.saveAsTextFile(params.outputPath)

    sc.stop()
  }

  def toWKT(input: String): String = {
    try {
      val replacedInput = input.replace("\\", "").drop(1).dropRight(1)
      val inputPolygon = OperatorImportFromJson
        .local()
        .execute(Geometry.Type.Polygon, replacedInput)
        .getGeometry()
      OperatorExportToWkt.local().execute(WktExportFlags.wktExportDefaults, inputPolygon, null)
    } catch {
      case e: Exception => ""
    }
  }

  case class Params(
    inputPath: String = "",
    outputPath: String = ""
  )

}
