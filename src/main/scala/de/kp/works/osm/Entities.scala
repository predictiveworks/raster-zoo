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
import de.kp.works.raster.BBox
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

abstract class Entities extends OSMFrame {

  protected final val HIGHWAY:String = "highway"
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

  protected def limitNodes(bbox:BBox):DataFrame = {
    limitNodes(bbox, nodePath)
  }

  protected def limitNodes(bbox:BBox, query:Query):DataFrame = {
    limitNodes(bbox, query, nodePath)
  }

  protected def limitWays(bbox:BBox):DataFrame = {
    limitWays(bbox, nodePath, wayPath)
  }

  protected def limitWays(bbox:BBox, query:Query):DataFrame = {
    limitWays(bbox, query, nodePath, wayPath)
  }

  protected def loadRelationMembers:DataFrame = {
    loadRelationMembers(relationPath)
  }

  protected def loadRelations:DataFrame = {
    loadRelations(relationPath)
  }

  protected def loadWays:DataFrame = {
    loadWays(wayPath)
  }

}
