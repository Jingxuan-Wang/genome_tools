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
  * @last editor: jingxuan
  * @last edit time: 1/5/20
  */
case class Coordinate(lat: Double, lon: Double)

object Coordinate{

  def haversine(pointA: Coordinate, pointB: Coordinate): Double = {
    val lat1: Double = math.toRadians(pointA.lat)
    val lng1: Double = math.toRadians(pointA.lon)
    val lat2: Double = math.toRadians(pointB.lat)
    val lng2: Double = math.toRadians(pointB.lon)
    val dLat = lat2 - lat1
    val dLon = lng2 - lng1

    val a = math.pow(math.sin(dLat / 2), 2) + math.pow(math.sin(dLon / 2), 2) * math.cos(lat1) * math.cos(lat2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    val r = 6378126.0 // Radius of earth in metres

    r * c
  }
}