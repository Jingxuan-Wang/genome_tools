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

import genome.util.{Trajectory, Update}
import timeOp.TimeCommon.{parse, roundingTime, zdtToString}

/**
  * ${MODULE_NAME} is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 3/6/20
 */


object RStayPrepare{
  def deriveToTimeSlot(trajectory: Trajectory, interval: Int = 300): Seq[(String, Seq[Update])] = {
    val groupedUpdates = trajectory.updates.groupBy{ u =>
      val timestamp = parse(u.timestamp, "Asia/Singapore")
      val roundedTimestamp = roundingTime(timestamp, interval = 300, rdown = true)
      zdtToString(roundedTimestamp)
    }.toSeq
    groupedUpdates.sortBy(_._1)
  }
}
