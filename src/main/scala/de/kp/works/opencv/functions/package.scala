package de.kp.works.opencv
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
import de.kp.works.geom.functions.geometryToBBox
import de.kp.works.geom.model.BBox
import org.locationtech.jts.geom.{Coordinate, Geometry => JtsGeometry}
import org.opencv.core.{Mat, Point, Scalar}
import org.opencv.imgproc.Imgproc

package object functions extends Serializable {

  /**
   * REMINDER: This call checks whether the OpenCV library
   * is loaded, and if not, loads it.
   */
  OpenCV.isOpenCVLoaded

  /** DRAW LINE **/

  /**
   * The default line is a green line of thickness = 1
   */
  def setLine(mat:Mat, sx:Int, sy:Int, ex:Int, ey:Int):Unit = {
    /* GREEN: BGR Coding */
    val color = new Scalar(0, 255, 0)
    setLine(mat, sx, sy, ex, ey, color, 1)
  }

  def setLine(mat:Mat, sx:Int, sy:Int, ex:Int, ey:Int, color:Scalar):Unit = {
    setLine(mat, sx, sy, ex, ey, color, 1)
  }

  def setLine(mat:Mat, sx:Int, sy:Int, ex:Int, ey:Int, color:Scalar, thickness:Int):Unit = {

    val sp = new Point(sx, sy)
    val ep = new Point(ex, ey)

    Imgproc.line(mat, sp, ep, color, thickness)

  }

  /** DRAW PIXEL **/

  /**
   * This method transforms a certain Jts Coordinate
   * within the boundaries of a specific bounding box
   * into pixel coordinates.
   */
  def coordinateToPixel(coord:Coordinate, dimension:Seq[Int], bbox:BBox): Seq[Int] = {

    val lon = coord.x
    val lat = coord.y

    val Seq(width, height) = dimension

    /* Extract bounding box */
    val minLon = bbox.minLon
    val maxLon = bbox.maxLon

    val minLat = bbox.minLat
    val maxLat = bbox.maxLat

    val x = math.round(((lon - minLon) / (maxLon - minLon)) * width)
    val y = math.round(((lat - minLat) / (maxLat - minLat)) * height)

    /**
     * We expect that the coordinates are non-negative numbers.
     * Negative numbers indicate that the provided point is outside
     * the boundaries
     */
    if (x < 0 || x > width || y < 0 || y > height) return null
    Seq(x.toInt, y.toInt)

  }

  /** DRAW GEOMETRY **/

  /**
   * This method draws the subsequent coordinates of
   * a Jts Geometry as lines on the OpenCV matrix.
   */
  def geometryToMat(
     /*
      * The matrix (e.g.) specifies the image
      * representation of a certain tile.
      */
     matrix:Mat,
     /*
      * The geometry (e.g.) specifies the hexagon
      * that represents a H3 index
      */
     geometry:JtsGeometry,
     /*
      * The width and height of the image of
      * a certain tile
      */
     dimension:Seq[Int],
     /*
      * The envelope that restricts a specific
      * tile.
      */
     envelope:JtsGeometry):Mat = {

    /*
     * The bounding box is required to compute
     * the pixel representation of a geospatial
     * point
     */
    val bbox = geometryToBBox(envelope)
    val coordinates = geometry.getCoordinates

    (0 until coordinates.size - 1).foreach(i => {
      /*
       * We compute the pixels of two subsequent points
       * to directly draw a line on the OpenCv matrix
       */
      val cPix = coordinateToPixel(coordinates(i), dimension, bbox)
      val nPix = coordinateToPixel(coordinates(i + 1), dimension, bbox)

      if (cPix == null || nPix == null) {
        /* Do nothing */

      } else {
        setLine(matrix, cPix.head, cPix(1), nPix.head, nPix(1))
      }

    })

    matrix

  }

}
