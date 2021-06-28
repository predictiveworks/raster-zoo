package de.kp.works.osm
/**
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */
import org.apache.spark.sql.DataFrame
import de.kp.works.spark.Session
/*
 * The [Member] case class determines each member that
 * specifies a certain relation.
 */
case class Member(mid:Long, mrole:String, mtype:String)
/*
 * The [Node] case class determines each node that
 * specifies a certain way with a specific relation.
 */
case class Node(nix:Int, nid:Long)

abstract class Entities {

  protected final val HIGHWAY:String = "highway"
  protected final val TAGS:String = "tags"
  /*
   * The list columns that are not taken into account
   */
  protected final val DROP_COLS:Seq[String] =
    List("timestamp", "changeset", "uid", "user_sid")

  protected val session = Session.getSession
  /*
   * The path to the OSM nodes parquet file.
   */
  protected var nodePath:String = ""
  /*
   * The path to the OSM relations parquet file.
   */
  protected var relationPath:String = ""
  /*
   * The path to the OSM ways parquet file.
   */
  protected var wayPath:String = ""
  /*
   * The path to the FS parquet folder that contains
   * intermediate and final computation results
   */
  protected var rasterZoo:String = ""

  def setNodePath(value:String): Entities = {
    nodePath = value
    this
  }

  def setRelationPath(value:String): Entities = {
    relationPath = value
    this
  }

  def setWayPath(value:String): Entities = {
    wayPath = value
    this
  }

  def setRasterZoo(value:String): Entities = {
    rasterZoo = value
    this
  }

  protected def loadNodes:DataFrame = {
    if (nodePath.isEmpty)
      throw new Exception("The path to the `nodes` parquet file is not provided.")

    session.read.parquet(nodePath)

  }

  protected def loadRelations:DataFrame = {
    if (relationPath.isEmpty)
      throw new Exception("The path to the `relations` parquet file is not provided.")

    session.read.parquet(relationPath)

  }

  protected def loadWays:DataFrame = {
    if (wayPath.isEmpty)
      throw new Exception("The path to the `ways` parquet file is not provided.")

    session.read.parquet(wayPath)

  }

}
