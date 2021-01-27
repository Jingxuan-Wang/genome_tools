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
  * This module is for testing trajectory
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last editor: jingxuan
  * @last edit time: 5/5/20
 */

import org.scalatest.FlatSpec
import genome.util.{SensorType, Trajectory, Update}

class TestingTrajectory extends FlatSpec{
  "Trajectory" should "convert string to Trajectory" in {
    val trajectroyStr = "A;152.92368213,-31.44835334,2019-12-31 13:06:59+0000,1,505-02-52254-19095817;152.92368213,-31.448353340000004,2019-12-31 13:29:29+0000,10,i"

    val trajectory = Trajectory.fromString(trajectroyStr)
    assert(trajectory.agentId == "A")
    assert(trajectory.updates == Seq(Update(lat = 152.92368213,lon = -31.44835334, timestamp = "2019-12-31 13:06:59+0000", cellType = SensorType.withName("1"), cell ="505-02-52254-19095817"), Update(lon = 152.92368213, lat = -31.448353340000004, timestamp = "2019-12-31 13:29:29+0000", cellType = SensorType.withName("10"), cell = "i") ))
  }
}
