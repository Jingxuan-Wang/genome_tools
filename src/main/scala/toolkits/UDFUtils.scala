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

import timeOp.TimeCommon._
import org.apache.spark.sql.functions.udf

/**
  * ${MODULE_NAME} is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 19/11/20
 */

object UDFUtils {

  val timeOverlapUDF = udf[Long, String, String, String, String](timeOverlap)
  /**
    * find overlap between two pairs of time
    * @param in_time1
    * @param in_time2
    * @param out_time1
    * @param out_time2
    * @return
    */
  def timeOverlap(in_time1: String, out_time1: String, in_time2: String, out_time2: String): Long = {
    if (in_time1 < out_time2 && in_time2 < out_time1){
      val max_in_time = in_time1 < in_time2 match {
        case true => in_time2
        case false => in_time1
      }
      val min_out_time = out_time1 < out_time2 match {
        case true => out_time1
        case false => out_time2
      }
      zdtFromString(min_out_time).toEpochSecond - zdtFromString(max_in_time).toEpochSecond}
    else{
      0L
    }
  }
}
