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

import com.intel.analytics.bigdl.opencv.OpenCV
import de.kp.works.h3.H3Utils
import de.kp.works.opencv.functions.geometryToMat
import de.kp.works.raster.Columns.geometry_col
import de.kp.works.spark.UDF
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.locationtech.jts.geom.{Geometry => JtsGeometry}
import org.opencv.core.{CvType, Mat, MatOfByte}
import org.opencv.imgcodecs.Imgcodecs

import scala.collection.mutable
/**
 * The [H3Indexer] leverages Uber's H3 indexing system
 * to assign geospatial hash indexes to a JTS Geometry
 */
class H3Indexer extends RasterParams {

  /**
   * REMINDER: This call checks whether the OpenCV library
   * is loaded, and if not, loads it.
   */
  OpenCV.isOpenCVLoaded

  var boundaryColName:String = "boundary"
  var bytesColName:String = "bytes"

  var h3IndexColName:String = "h3_index"
  /*
   * Indexing can be performed by exploding the
   * assigned H3 indexes or not. For spatial joins
   * based on these indexes, isExploded must be true.
   *
   * For other use cases, like visualizing the hexagons
   * it is recommended to set isExploded = false
   */
  var isExploded = false

  def setIsExploded(value:Boolean):H3Indexer = {
    isExploded = value
    this
  }

  def setBoundaryCol(name:String):H3Indexer = {
    boundaryColName = name
    this
  }

  def setBytesCol(name:String):H3Indexer = {
    bytesColName = name
    this
  }

  def setH3IndexCol(name:String):H3Indexer = {
    h3IndexColName = name
    this
  }

  def setIndexCol(name:String):H3Indexer = {
    setIndexColName(name)
    this
  }

  def setRasterCol(name:String):H3Indexer = {
    setRasterColName(name)
    this
  }

  /**
   * This method is used to set the global resolution
   * for indexing the boundaries (polygon) of each
   * tile.
   */
  def setResolution(value:Int):H3Indexer = {
    setResolutionValue(value)
    this
  }
  /**
   * An indexed rasterframe can be the starting
   * point to join with other data sources like OSM
   */
  def transform(rasterframe:DataFrame):DataFrame = {

    validate()
    /**
     * STEP #1: Assign a H3 compliant resolution
     * to each tile of the rasterframe. This info
     * is used to enable appropriate geospatial
     * indexing.
     *
     * It also prepares ground for connecting with
     * other geospatial dataframes.
     */
    var dataset = RasterUtil
      .computeResolution(rasterframe, rasterColName)
      /*
       * Restrict the dataset to those tiles that need
       * smaller resolutions to be covered by a hexagon
       * that the selected global resolution.
       */
      .filter(col(resolutionColName) < resolution)
    /**
     * STEP #2: Extract the polygon representation
     * of the raster tiles and index the polygon that
     * describes the boundary of a certain tile with H3
     */
    dataset = if (isExploded) {
      dataset.withColumn(h3IndexColName,
        explode(H3Utils.envelopeJts_to_H3(resolution)(geometry_col(rasterColName))))
    }
    else {
      dataset.withColumn(h3IndexColName,
        H3Utils.envelopeJts_to_H3(resolution)(geometry_col(rasterColName)))

    }

    dataset

  }

  /**
   * This method expects that isExploded = false.
   *
   * It draws hexagons on the OpenCv matrix representation
   * that refer to the selected resolution. This method e.g.
   * supports a use case where the hexagon coverage of each
   * tile must visualized.
   */
  def hexagonsToTiles(input:DataFrame):DataFrame = {

    if (isExploded)
      throw new Exception(s"Assigning hexagons to tiles is not supported for exploded indexes.")

    /*
     * Assign boundary to each tile
     */
    val dataset = input
       .withColumn(boundaryColName, UDF.get_envelope(geometry_col(rasterColName)))

    val schema = StructType(dataset.schema.fields ++
      Array(StructField(bytesColName, BinaryType, nullable = false)))

    val session = dataset.sparkSession
    val sc = session.sparkContext
    /*
     * Defined and broadcast metadata needed to
     * work with each row of the RDD. Hexagons
     * are configured as 8-Bit BGR `png` images.
     */
    val metadata = sc.broadcast(Map(
      /* Column names to extract data */
      "boundary.col" -> boundaryColName,
      "height.col"   -> heightColName,
      "index.col"    -> h3IndexColName,
      "width.col"    -> widthColName,
      /* Unsigned 8-Bit and 3-Channel (BGR) image */
      "cv.type"      -> CvType.CV_8UC3.toString,
      "extension"    -> "png"
    ))

    val rdd = dataset.rdd.map(row => {

      val md = metadata.value
      /**
       * STEP #1: Extract `width` and `height` for
       * each tile and initialize an OpenCV matrix
       */
      val width  = row.getAs[Int](md("width.col"))
      val height = row.getAs[Int](md("height.col"))

      val cvType = md("cv.type").toInt
      var mat = Mat.zeros(height, width, cvType)
      /**
       * STEP #2: Extract the sequence of H3 indexes,
       * and convert each index into the polygon that
       * specifies the associated hexagon
       */
      val boundary = row.getAs[JtsGeometry](md("boundary.col"))

      val indexes = row.getAs[mutable.WrappedArray[Long]](md("index.col"))
      indexes.foreach(index => {
        val geom = H3Utils.H3ToBoundaryJts(index)
        mat = geometryToMat(mat, geom, Seq(width, height), boundary)
      })
      /**
       * STEP #3: Transform matrix into byte array
       * that refers to a certain file image extension
       */
      val extension = md("extension")

      val matOfByte = new MatOfByte()
      Imgcodecs.imencode(s".$extension", mat, matOfByte)

      val bytes = matOfByte.toArray
      /**
       * STEP #4: Append byte array to the provided row
       */
      val values = row.toSeq ++ Seq(bytes)
      Row.fromSeq(values)

    })

    session.createDataFrame(rdd, schema)

  }

  private def validate():Unit = {

    if (resolution == -1)
      throw new Exception(s"The resolution for indexing the rasterframe was not set.")
  }
}
