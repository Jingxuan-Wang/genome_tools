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

package genome.util

import com.typesafe.config.{Config, ConfigFactory}
import timeOp.TimeCommon._
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * ${MODULE_NAME} is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 29/10/20
  */

object LBSRecord {

  val defMap = Map(
    "LEN_LINE"         -> 8,
    "INDEX_TIMESTAMP_SG"  -> 0,
    "INDEX_TIMESTAMP_AU"  -> 8,
    "INDEX_IMSI"       -> 1,
    "INDEX_MSISDN"     -> 2,
    "INDEX_CELL"       -> 3,
    "INDEX_EVENT_TYPE" -> 4,
    "INDEX_LAT"        -> 5,
    "INDEX_LON"        -> 6,
    "INDEX_IMEI"       -> 7,
    "INDEX_LAC"        -> 2,
    "INDEX_CELL_ID"    -> 3,
    "SEP"              -> "\\|",
    "CELLSEP"          -> "-"
  )
  val defConf = ConfigFactory.parseMap(defMap.asJava)
  val defParser = getParser(defConf)

  def getParser(config: Config): LBSParser = {
    config.withFallback(defConf)
    LBSParser(
      INDEX_TIMESTAMP_SG = config.getInt("INDEX_TIMESTAMP_SG"),
      INDEX_TIMESTAMP_AU = config.getInt("INDEX_TIMESTAMP_AU"),
      INDEX_IMSI = config.getInt("INDEX_IMSI"),
      INDEX_MSISDN = config.getInt("INDEX_MSISDN"),
      INDEX_CELL = config.getInt("INDEX_CELL"),
      INDEX_EVENT_TYPE = config.getInt("INDEX_EVENT_TYPE"),
      INDEX_LAT = config.getInt("INDEX_LAT"),
      INDEX_LON = config.getInt("INDEX_LON"),
      INDEX_IMEI = config.getInt("INDEX_IMEI"),
      SEP = config.getString("SEP"),
      INDEX_LAC = config.getInt("INDEX_LAC"),
      INDEX_CELL_ID = config.getInt("INDEX_CELL_ID"),
      CELLSEP = config.getString("CELLSEP"),
      LEN_LINE = config.getInt("LEN_LINE")
    )
  }

  /**
    * Parses a string to LBSRecord. Returns `false` if not all the records can be assigned.
    */
  def fromString(line: String, timeZone: String): Option[LBSRecord] = {
    defParser.fromStringRobust(line, timeZone = timeZone)
  }

}

case class LBSRecord(
  location: String = "",
  datetime: String,
  cgi: String,
  lac: String = "",
  cellID: String = "",
  eventType: String,
  msisdn: String = "",
  imei: String,
  agentID: String,
  lat: Option[Double],
  lon: Option[Double],
  gridID: Int = 0
) {

  def getCellKey() = List(lac, cellID).mkString("-")

  def getCellFullKey() = cgi

  def withCoords(newLat: Double, newLon: Double): LBSRecord = {
    LBSRecord(
      agentID = agentID,
      location = newLat + "," + newLon,
      datetime = datetime,
      cgi = cgi,
      lac = lac,
      cellID = cellID,
      eventType = eventType,
      msisdn = msisdn,
      imei = imei,
      lat = Some(newLat),
      lon = Some(newLon)
    )
  }

  override def toString() = {
    List(datetime, agentID, msisdn, cgi, eventType, lat.getOrElse(""), lon.getOrElse(""), imei)
      .mkString("|")
  }
}

case class LBSParser(
  INDEX_TIMESTAMP_SG: Int = 0,
  INDEX_TIMESTAMP_AU: Int = 8,
  INDEX_IMSI: Int = 1,
  INDEX_MSISDN: Int = 2,
  INDEX_CELL: Int = 3,
  INDEX_EVENT_TYPE: Int = 4,
  INDEX_LAT: Int = 5,
  INDEX_LON: Int = 6,
  INDEX_IMEI: Int = 7,
  SEP: String = "\\|",
  INDEX_LAC: Int = 2,
  INDEX_CELL_ID: Int = 3,
  CELLSEP: String = "-",
  LEN_LINE: Int = 8
) {

  def fromStringRobust(line: String, timeZone: String): Option[LBSRecord] = {
    try {
      val fields = line.split(SEP, -1)
      val lat = Try(fields(INDEX_LAT).toDouble).toOption
      val lon = Try(fields(INDEX_LON).toDouble).toOption

      if (fields.length < LEN_LINE) None

      val dt = timeZone match {
        case "Asia/Singapore" => zdtToString(parseToUTC(fields(INDEX_TIMESTAMP_SG), timeZone))
        case "Australia/Sydney" => zdtToString(parseToUTC(fields(INDEX_TIMESTAMP_AU), timeZone))
      }

      val cgi = fields(INDEX_CELL)
      val cgiComponents = cgi.split(CELLSEP, -1)
      val (lac, cellID) =
        if (cgiComponents.length >= 4)
          (cgiComponents(INDEX_LAC), cgiComponents(INDEX_CELL_ID))
        else ("", "")
      val lbs = LBSRecord(
        agentID = fields(INDEX_IMSI),
        location = fields(INDEX_LAT) + "," + fields(INDEX_LON),
        datetime = dt,
        cgi = cgi,
        lac = lac,
        cellID = cellID,
        eventType = fields(INDEX_EVENT_TYPE),
        msisdn = fields(INDEX_MSISDN),
        imei = fields(INDEX_IMEI),
        lat = lat,
        lon = lon
      )
      Some(lbs)
    } catch {
      case e: Exception => None
    }
  }
}
