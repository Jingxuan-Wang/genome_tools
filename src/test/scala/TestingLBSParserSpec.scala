import genome.util.LBSRecord
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

/**
  * ${MODULE_NAME} is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 18/11/20
 */


class TestingLBSParserSpec extends FlatSpec{
  "LBSRecord" should "convert lbs string to LBSRecord" in {
    val lbsStr1 = "|62AED5D7FE7DB00E6FACE11AE0A28B46BD09D9D6239813A36A41D93685083A8F|niometrics_location_events|505-02-53370-21348106|S11|-38.218597|146.247389|Moe Hill-O:M0125-OL18-H-1|2020-10-25 10:09:28.000|2020-10-25 10:09:28.000"

    val res = LBSRecord.fromString(lbsStr1, "Australia/Sydney")
    assert(res.nonEmpty)
    assert(res.get.agentID == "62AED5D7FE7DB00E6FACE11AE0A28B46BD09D9D6239813A36A41D93685083A8F")
    assert(res.get.datetime == "2020-10-24 23:09:28+0000")
    assert(res.get.lat == Some(-38.218597))
    assert(res.get.lon == Some(146.247389))
  }
}
