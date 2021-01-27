package genome

import java.io.FileWriter

import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import toolkits.SparkCommon.readCSV

/**
  *
  * @author jingxuan
  * @since 11/12/19
  *
  */

object CellInventory {

  case class Inventory(
    siteId: String,
    siteName: String,
    lac: String,
    lgaName: String,
    siteType: String,
    cellLon: Option[Double],
    cellLat: Option[Double],
    technology: String,
    band: String,
    cellSector: String,
    cellId: String,
    cellName: String,
    sacId: String,
    sa3Code: String,
    rncBsc: String,
    cellFirstSeen: String,
    antennaAzimuth: Option[Int],
    beamWidthHorizontal: Option[Int],
    beamWidthVertical: Option[Int],
    antennaElectricDownTilt: String,
    antennaMechanicalDownTilt: String,
    antennaHeight: String,
    antennaGain: String,
    antennaType: String,
    cellPower: String,
    feederLength: String,
    feederSize: String,
    cellStatus: String
  )

  final val colNames = Seq()

  case class Params(
    inputPath: String = "",
    date: String = "",
    outputPath: String = ""
  )

  val appName = this.getClass.getSimpleName

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Params](appName) {
      opt[String]("input")
        .text("Input directory")
        .action((x, c) => c.copy(inputPath = x))

      opt[String]("date")
        .text("date")
        .action((x, c) => c.copy(date = x))

      opt[String]("output")
        .text("Output directory")
        .action((x, c) => c.copy(outputPath = x))
    }
    parser.parse(args, Params()).map { params =>
      process(params)
    }
  }

  def fullCellId(cell: Inventory): Option[String] = {
    if (cell.lac != null) {
      val cellID = cell.technology match {
        case "UMTS" => "505-02-" + cell.lac + "-" + cell.sacId
        case "LTE"  => "505-02-" + cell.lac + "-" + cell.cellId
        case _      => ""
      }
      Some(cellID)
    } else {
      None
    }
  }

  def process(params: Params): Unit = {
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._

    val date = params.date
    val dataset = readCSV[Inventory](spark, params.inputPath, delimiter = "|")

    val cell = dataset
      .map(r => (fullCellId(r), r.siteType, r.cellStatus))
      .filter(x => x._1.isDefined && x._3 == "ACTIVE")
    val cellCount = cell.distinct().count()
    val inBuildingCount =
      cell.filter(r => r._2 == "In Building").distinct().count()
    val outputString = Seq(date, cellCount, inBuildingCount).mkString(",")
    val fw = new FileWriter(params.outputPath, true)
    fw.write(outputString + "\n")
    fw.close()

    spark.stop()
  }

}
