/*
 * Copyright © DataSpark Pte Ltd 2014 - 2020.
 *
 * This software and any related documentation contain confidential and proprietary information of
 * DataSpark and its licensors (if any). Use of this software and any related documentation is
 * governed by the terms of your written agreement with DataSpark. You may not use, download or
 * install this software or any related documentation without obtaining an appropriate licence
 * agreement from DataSpark.
 *
 * All rights reserved.
 */

package genome

import java.io.File

import com.github.kxbmap.configs._
import com.typesafe.config.{Config, ConfigFactory}
import genome.util.{LBSRecord, SensorType, Update}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import timeOp.TimeCommon._

import scala.util.Try

/**
  * ${MODULE_NAME} is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 3/6/20
  */

object LBSToTrajectory {

  val appName = this.getClass.getSimpleName

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Params](appName) {
      opt[String]("input")
        .text("Input directory")
        .action((x, c) => c.copy(inputPath = x))

      opt[String]("cell")
        .text("cell directory")
        .action((x, c) => c.copy(cellPath = x))

      opt[String]("config")
        .text("cell directory")
        .action((x, c) => c.copy(configPath = x))

      opt[String]("output")
        .text("Output directory")
        .action((x, c) => c.copy(outputPath = x))
    }
    parser.parse(args, Params()).map { params =>
      process(params)
    }
  }

  def process(params: Params): Unit = {
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    val sc = spark.sparkContext

    val config: Config = ConfigFactory.parseFile(new File(params.configPath))
    val ID_REGEX =
      config.get[String]("ID_REGEX").r.pattern

    val lbsParser =
      if (config.hasPath("LBS")) {
        LBSRecord.getParser(config.getConfig("LBS"))
      } else LBSRecord.defParser

    val cgiToCellInfo: Map[String, CellInfo] =
      if (params.cellPath.isEmpty) Map()
      else {
        sc.textFile(params.cellPath, 100)
          .map(s => s.split("\\|", -1))
          .map { arr =>
            arr.length match {
              case 12 =>
                arr(0) -> CellInfo(
                  cgi = arr(0),
                  lat = arr(3).toDouble,
                  lon = arr(4).toDouble,
                  isKDE = Try(arr(9).toBoolean).getOrElse(false),
                  isIndoor = Try(arr(10).toBoolean).getOrElse(false),
                  buildingName = Try(arr(11)).getOrElse("Unknown")
                )
              case _ =>
                arr(0) -> CellInfo(
                  cgi = arr(0),
                  lat = arr(3).toDouble,
                  lon = arr(4).toDouble,
                  isKDE = false,
                  isIndoor = false,
                  buildingName = Try(arr(11)).getOrElse("Unknown")
                )
            }
          }
          .collectAsMap
          .toMap
      }

    def clearCoordinates(l: Option[LBSRecord]): Option[LBSRecord] = {
      if (l.isEmpty) return None
      val lb = l.get
      val cellInfo = cgiToCellInfo.get(lb.cgi)
      if (cellInfo.isEmpty) {
        if (lb.lat.isDefined && lb.lon.isDefined)
          return l
        else
          return None
      }
      Some(lb.withCoords(cellInfo.get.lat, cellInfo.get.lon))
    }

    val inputRDD = sc.textFile(params.inputPath)

    inputRDD
      .map(lbsParser.fromStringRobust(_, "Asia/Singapore"))
      .map(clearCoordinates)
      .flatMap(x => x)
      .filter(x => ID_REGEX.matcher(x.agentID).matches())
      .map(x =>
        (
          x.agentID,
          Update(
            x.lon.getOrElse(1000),
            x.lat.getOrElse(1000),
            zdtToLbsString(x.datetime),
            SensorType.CellTower,
            x.cgi
          )
        )
      )
      .aggregateByKey(List[Update]())((list, v) => v :: list, (list1, list2) => list1 ::: list2)
      .map(r =>
        r._1 + ";" + r._2.sortBy(u=>u.timestamp).map(i => i.toString).mkString(";")
      ).saveAsTextFile(params.outputPath)

    spark.stop()
  }

  case class Params(
    inputPath: String = "",
    cellPath: String = "",
    configPath: String = "",
    outputPath: String = ""
  )

  case class CellInfo(
    cgi: String,
    lat: Double,
    lon: Double,
    isKDE: Boolean,
    isIndoor: Boolean,
    buildingName: String
  )

}
