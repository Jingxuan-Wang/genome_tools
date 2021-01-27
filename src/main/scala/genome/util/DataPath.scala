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

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
  * ${MODULE_NAME} is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 18/5/20
 */

object DataPath {
  final val archive: String = "s3a://au-daas-archive"
  final val compute: String = "s3a://au-daas-compute/output"

  final val archiveBucketMap: Map[String, String] = Map(
    "lbs" -> s"$archive/lbs-transformed",
    "trajectory" -> s"$archive/trajectory",
    "staypoint" -> s"$archive/staypoint",
    "trip" -> s"$archive/trip")

  final val computeBucketMap: Map[String, String] = Map(
    "lbs" -> s"s3a://au-daas-compute/input/lbs-transformed",
    "trajectory" -> s"$compute/raw/trajectory",
    "staypoint" -> s"$compute/parquet/staypoint",
    "trip" -> s"$compute/parquet/trip",
    "geo_hierarchy" -> s"$compute/parquet/aggregated-geo-hierarchy" )

  final val roamerWeight = "s3a://au-daas-compute/xtrapolation_for_roamer/roamer_imsi_weight"


  def
  getPath(spark: SparkSession, date:String, module:String = "lbs"): Option[String] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val archiveBucket = archiveBucketMap.getOrElse(module, "")
    val computeBucket = computeBucketMap.getOrElse(module, "")
    val archivePath = s"$archiveBucket/$date"
    val computePath = s"$computeBucket/$date"
    val archiveFS = FileSystem.get(URI.create(archiveBucket), conf)
    val computeFS = FileSystem.get(URI.create(computeBucket), conf)
    if(computeFS.exists(new Path(computePath))){
      Some(computePath)
    }
    else if (archiveFS.exists(new Path(archivePath))){
      Some(archivePath)
    }
    else{
      None
  }}
}
