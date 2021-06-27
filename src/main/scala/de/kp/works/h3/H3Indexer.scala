package de.kp.works.h3

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

import de.kp.works.raster.Columns.geometry_col
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * The [H3Indexer] leverages Uber's H3 indexing system
 * to assign geospatial hash indexes to a JTS Geometry
 */
class H3Indexer {

  var indexColName:String  = "h3index"
  var rasterColName:String = "proj_raster"
  var uuidColName:String   = "miid"

  val geometryColName:String = "_geometry"

  var resolution:Int = 7

  def setIndexCol(name:String):H3Indexer = {
    indexColName = name
    this
  }

  def setRasterCol(name:String):H3Indexer = {
    rasterColName = name
    this
  }

  def setUUIDCol(name:String):H3Indexer = {
    uuidColName = name
    this
  }

  def setResolution(value:Int):H3Indexer = {
    resolution = value
    this
  }
  /**
   * An indexed rasterframe can be the starting
   * point to join with other data sources like OSM
   */
  def transform(rasterframe:DataFrame):DataFrame = {
    /**
     * STEP #1: Extract the polygon representation
     * of the raster tiles
     */
    val geometry = rasterframe
      .withColumn(geometryColName, geometry_col(rasterColName))
      .select(uuidColName, geometryColName)
    /**
     * STEP #2: Index the polygon that describes the
     * boundary of a certain tile with H3
     */
    val indexed = geometry
      .withColumn(indexColName, explode(H3Utils.boundaryToH3(resolution)(col(geometryColName))))

    indexed.drop(geometryColName)

  }
}
