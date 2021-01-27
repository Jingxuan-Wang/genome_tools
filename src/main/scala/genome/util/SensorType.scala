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
  *
  * @author jingxuan
  * @since 2019-05-30
  *
  */
object SensorType extends Enumeration{
  type SensorType = Value
  val Unknown = Value(0)
  val CellTower = Value(1)
  val Wifi = Value(2)
  val GPS = Value(3)
  val Interpolated = Value(10)
}

