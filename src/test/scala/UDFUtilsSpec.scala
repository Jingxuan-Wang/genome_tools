import org.scalatest.FlatSpec

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
import toolkits.UDFUtils._


/**
  * This module is for testing functions in UDFUntils
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 19/11/20
 */


class UDFUtilsSpec extends FlatSpec {
  "Time Overlap" should "return correct duration in second if there is a overlap" in{
    val time1 = "2020-10-19 00:00:01+0000"
    val time2 = "2020-10-19 00:00:02+0000"
    val time3 = "2020-10-19 00:00:03+0000"
    val time4 = "2020-10-19 00:00:04+0000"
    // in_time1 < in_time2 < out_time1 < out_time2
    val case1 = timeOverlap(time1, time3, time2, time4)
    assert(case1 == 1L)
    // in_time2 < in_time1 < out_time2 < out_time1
    val case2 = timeOverlap(time2, time4, time1, time3)
    assert(case2 == 1L)
    // in_time1 < in_time2 < out_time2 < out_time1
    val case3 = timeOverlap(time1, time4, time2, time3)
    assert(case3 == 1L)
    // in_time2 < in_time1 < out_time1 < out_time2
    val case4 = timeOverlap(time2, time3, time1, time4)
    assert(case4 == 1L)
    // in_time1 < out_time1 < in_time2 < out_time2
    val case5 = timeOverlap(time1, time2, time3, time4)
    assert(case5 == 0L)
  }
}
