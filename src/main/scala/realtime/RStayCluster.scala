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

package realtime

import java.time.ZonedDateTime
import math.{cos, toRadians, sin, atan2, sqrt, toDegrees}

import genome.util.{Coordinate, Update}
import genome.util.Coordinate.haversine
import toolkits.Utils.percentile
import timeOp.TimeCommon.{parse, timeDifference}
/**
  * ${MODULE_NAME} is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 3/6/20
 */

case class RStayCluster (
                          agentID: String,
                          startTime: ZonedDateTime,
                          endTime: ZonedDateTime,
                          centroid: Coordinate,
                          updates: Seq[Update],
                          status: String = "",
                          distanceThreshold: Long,
                          durationThreshold: Long
                        ){
  def duration: Double = {
    timeDifference(startTime, endTime)
  }

  def raidus: Double = {
    updates.map(u => haversine(Coordinate(lat = u.lat, lon = u.lon), centroid)).max
  }

  def setStatus: RStayCluster = {
    if (this.raidus <= distanceThreshold / 2){
      this.copy(status = "stay")
    }else if (this.raidus > this.distanceThreshold/2 & this.raidus < this.distanceThreshold){
      this.copy(status = "transit")
    }else{
      this.copy(status = "move")
    }
  }
}

object RStayCluster{
  def init(agentID: String, updates: Seq[Update], config: Map[String, Long]): RStayCluster = {
    val sortedUpdates = updates.sortBy(_.timestamp)
    val startTime = parse(sortedUpdates.head.timestamp, "Asia/Singapore")
    val endTime = parse(sortedUpdates.last.timestamp, "Asia/Singapore")
    val centroid = getCentroid(sortedUpdates)
    val distanceThreshold = config("distance_threshold")
    val durationThreshold = config("duration_threshold")

    RStayCluster(agentID = agentID,
      startTime = startTime,
      endTime = endTime,
      centroid = centroid,
      updates = sortedUpdates,
      distanceThreshold = distanceThreshold,
      durationThreshold = durationThreshold)
  }


  def getCentroid(updates: Seq[Update], method: String = "mean"): Coordinate = {
    val listSize = updates.length
    val cLat = updates.map(u => toRadians(u.lat))
    val cLon = updates.map(u => toRadians(u.lon))
    val xList = cLat.zip(cLon).map(c => cos(c._1)*cos(c._2))
    val yList = cLat.zip(cLon).map(c => cos(c._1)*sin(c._2))
    val zList = cLat.map(c => sin(c))
    method match {
      case "mean" => {
        val x = xList.sum / listSize
        val y = yList.sum / listSize
        val z = zList.sum / listSize

        val lon = atan2(y, x)
        val hyp = sqrt(x*x + y*y)
        val lat = atan2(z, hyp)
        Coordinate(lat = toDegrees(lat), lon = toDegrees(lon))
      }
      case "median" => {
        val x = percentile(xList, 0.5)
        val y = percentile(yList, percent = 0.5)
        val z = percentile(zList, percent = 0.5)

        val lon = atan2(y, x)
        val hyp = sqrt(x*x + y*y)
        val lat = atan2(z, hyp)
        Coordinate(lat = toDegrees(lat), lon = toDegrees(lon))
      }
    }
  }

  def getRadius(updates: Seq[Update]): Double = {
    val centroid = getCentroid(updates)
    val radius = updates.map(u => haversine(Coordinate(lat=u.lat, lon=u.lon), centroid)).max
    radius
  }
}

