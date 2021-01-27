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


/**
  * JX-Analysis is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 1/5/20
  */
case class Update(
  lon: Double,
  lat: Double,
  timestamp: String,
  cellType: SensorType.Value,
  cell: String
) {
  def coordinate: Coordinate = Coordinate(this.lat, this.lon)

  override def toString: String = {
    List(lon.toString, lat.toString, timestamp, cellType.id.toString, cell).mkString(",")
  }
}

object Update {

  def fromString(s: String): Update = {
    val fields = s.split(",")
    new Update(
      lon = fields(0).toDouble,
      lat = fields(1).toDouble,
      timestamp = fields(2),
      cellType = SensorType.apply(fields(3).toInt),
      cell = fields(4)
    )
  }
}
