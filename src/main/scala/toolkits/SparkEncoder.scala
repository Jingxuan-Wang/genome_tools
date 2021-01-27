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
import org.apache.spark.sql.{
  Encoder,
  Encoders,
  SQLContext,
  SQLImplicits,
  SparkSession
}

import scala.reflect.ClassTag

/**
  * JX-Analysis is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last editor: jingxuan
  * @last edit time: 1/5/20
  */
trait SparkEncoder {
  var spark: SparkSession = _
  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }
  import testImplicits._

  implicit def single[A](implicit c: ClassTag[A]): Encoder[A] =
    Encoders.kryo[A](c)

  implicit def tuple2[A1, A2](implicit e1: Encoder[A1],
                              e2: Encoder[A2]): Encoder[(A1, A2)] =
    Encoders.tuple[A1, A2](e1, e2)

  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1],
                                  e2: Encoder[A2],
                                  e3: Encoder[A3]): Encoder[(A1, A2, A3)] =
    Encoders.tuple[A1, A2, A3](e1, e2, e3)
}
