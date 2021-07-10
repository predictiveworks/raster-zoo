package de.kp.works.raster

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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.locationtech.rasterframes.datasource.raster._
import de.kp.works.spark.Session

class RasterAnnotator {

  private val session = Session.getSession

  private var uri:String = ""
  private var boundingBox:Option[BBox] = None

  private var rasterColName:String = ""

  def setRasterColName(name:String):RasterAnnotator = {
    rasterColName = name
    this
  }

  def setUri(value:String):RasterAnnotator = {
    uri = value
    this
  }

  def annotate:DataFrame = {

    if (uri.isEmpty)
      throw new Exception("No `uri` provided.")
    /**
     * STEP #1: Load a certain COG file and
     * index for subsequent processing.
     */
    val rasterframe = loadFromUri
    /**
     * STEP #2: Compute the overall bounding box of
     * the respective COG file and its tiles.
     *
     * Note, in case of multi-band (channel) images,
     * it is expected that the geospatial bounding
     * box is always the same.
     *
     * The bounding box is the starting point to
     * filter or limit OSM data that refer to the
     * loaded rasterframe
     */
    boundingBox = Option(RasterUtil
      .computeBBox(rasterframe, rasterColName))

    // TODO
    null
  }
  /**
   * The simplest way to use the raster reader is with
   * a single raster from a single URI or file. The file
   * should be a valid Cloud Optimized GeoTIFF (COG),
   * which RasterFrames fully supports.
   *
   * RasterFrames will take advantage of the optimizations
   * in the COG format to enable more efficient reading
   * compared to non-COG GeoTIFFs.
   */
  private def loadFromUri:DataFrame = {

    val spec = Window.orderBy("miid")
    session.read.raster.load(uri)
      /*
       * The RasterFrame is annotated by Apache Spark's
       * monotonically increasing identifier to support
       * join operations with derived datasets like H3
       * indexed tiles.
       */
      .withColumn("miid", monotonically_increasing_id())
      /*
       * The next step is to assign an index of the ordered
       * identifiers. This is done in preparation of a final
       * merging with deep learning results from Analytics-Zoo
       */
      .withColumn("index", row_number().over(spec))
      .withColumn("index", col("index") - 1)
      .drop("miid")

  }

}
