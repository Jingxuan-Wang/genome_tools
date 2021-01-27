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

package toolkits

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * ${MODULE_NAME} is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 10/11/20
 */

object SqlUtils {

  /**
    * split grid column to lat/lon column or a coord column with lat & lon
    * @param grid
    * @param target
    * @return
    */
  def gridToCoord(grid: Column, target: String ="lat") : Column ={
    target match {
      case "lon" => split(grid, "_")(3).cast(DoubleType)
      case "lat" => split(grid, "_")(4).cast(DoubleType)
      case "all" => struct(split(grid, "_")(4).as("lat"), split(grid, "_")(3).as("lon"))
    }
  }

  /**
    * extract any part from UTC string
    * @param utc
    * @param target
    * @return
    */
  def extractTime(utc: Column, target: String = "hour"): Column = {
    target match {
      case "hour" => substring(utc, 12, 2).cast(IntegerType)
      case "minute" => substring(utc, 15, 2).cast(IntegerType)
      case "day" => substring(utc, 9, 2).cast(IntegerType)
      case "month" => substring(utc, 6, 2).cast(IntegerType)
      case "year" => substring(utc, 1, 2).cast(IntegerType)
      case "date" => split(utc, " ")(0).cast(StringType)
    }
  }

  /**
    * check if two pairs of time has overlaop or not
    * @param in_time1
    * @param out_time1
    * @param in_time2
    * @param out_time2
    * @return boolean
    */
  def isOverlap(in_time1:Column, out_time1: Column, in_time2: Column, out_time2: Column): Column = {
    in_time1 < out_time2 && in_time2 < out_time1
  }
}
