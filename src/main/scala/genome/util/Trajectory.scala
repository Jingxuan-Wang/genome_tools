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
  * This module is for
  *
  * @author: jingxuan
  * @maintainer: jingxuan
  * @last_editor: jingxuan
  * @last_edit_time: 1/5/20
  */
/**
  * Constructor for Trajectory
  * @param agentId(String): input for agentId
  * @param updates(Seq[Update]): input for updates
  */
class Trajectory(var agentId: String, var updates: Seq[Update]) extends Serializable{
  override def toString: String = {
    List.concat(Seq(this.agentId), this.updates.map(u => u.toString)).mkString(";")
  }
}

object Trajectory {
  /**
    * constructing method for trajectory
    * @param s input String
    * @return (agentID, updates)
    */
  def fromString(s: String): Trajectory = {
    val fields = s.split(";")
    val agentId = fields(0)
    val updates = fields.drop(1).map(Update.fromString).toSeq
    val tr = new Trajectory(agentId, updates)
    tr
  }

  def filterByTime(tr: Trajectory, stime: String, etime: Option[String] ): Trajectory = {
    val newUpdates = tr.updates.filter(u => {
      etime match {
        case Some(etime) => u.timestamp >= stime & u.timestamp <= etime
        case None => u.timestamp >= stime
      }})
    new Trajectory(tr.agentId, newUpdates)
  }

}
