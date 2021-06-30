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
import java.io.ByteArrayInputStream
import java.nio.file.Paths
import javax.imageio.ImageIO

import org.opencv.core.{CvType, Mat, MatOfByte, Point, Scalar}
import org.opencv.imgcodecs.Imgcodecs
import org.opencv.imgproc.Imgproc

import com.intel.analytics.bigdl.opencv.OpenCV

import de.kp.works.osm.Geometry
import de.kp.works.raster.BBox

class Renderer {

  var width:Int =  256
  var height:Int = 256

  /* RED: BGR Coding */
  var color:Scalar = new Scalar(0, 0, 255)

  /* Unsigned 8-Bit and 3-Channel (BGR) image */
  var cvType:Int = CvType.CV_8UC3

  var thickness:Int = 1

  def setCvType(value:Int):Renderer = {
    cvType = value
    this
  }

  def setColor(value:Scalar):Renderer = {
    color = value
    this
  }

  def setWidth(value:Int):Renderer = {
    width = value
    this
  }

  def setHeight(value:Int):Renderer = {
    height = value
    this
  }

  def setThickness(value:Int):Renderer = {
    thickness = value
    this
  }

  /**
   * REMINDER: This call checks whether the OpenCV library
   * is loaded, and if not, loads it.
   */
  OpenCV.isOpenCVLoaded

  def geometry2Mat(matrix:Mat, bbox:BBox, geometry:Geometry):Mat = {
    /**
     * STEP #1: Create OpenCV matrix that represents
     * the background of the image
     */
    val mat = if (matrix == null)
      initMat else matrix
    /**
     * STEP #2: Extract geometry
     */
    val `type` = geometry.geometry
    if (`type` == "linestring" || `type` == "polygon") {
      /**
       * STEP #3: Convert every geospatial point
       * into its pixel coordinates and set pixel
       */
      val coordinates = geometry.path
      (0 until coordinates.size - 1).foreach(i => {

        val cPix = latlon2Pixel(coordinates(i), bbox)
        val nPix = latlon2Pixel(coordinates(i + 1), bbox)

        setLine(mat, cPix.head, cPix(1), nPix.head, nPix(1))

      })

      mat

    } else
      throw new Exception(s"Geometry type '${`type`}' is not supported")

  }

  def initMat:Mat = {
    Mat.zeros(height, width, cvType)
  }

  private def setLine(mat:Mat, sx:Int, sy:Int, ex:Int, ey:Int):Unit = {

    val sp = new Point(sx, sy)
    val ep = new Point(ex, ey)

    Imgproc.line(mat, sp, ep, color, thickness)

  }
  /**
   * A helper method to set a certain colored pixel
   * at point (x,y) to an OpenCV matrix
   */
  private def setPixel(mat:Mat, x:Int, y:Int):Unit = {

    /* Pixel point: center of a circle */
    val pixel = new Point(x, y)

    val radius = 0
    /* Negative thickness indicates to fill the circle */
    val thickness = -1

    Imgproc.circle(mat, pixel, radius, color, thickness)

  }
  private def latlon2Pixel(point:Seq[Double], bbox:BBox):Seq[Int] = {

    val lat = point.head
    val lon = point(1)

    latlon2Pixel(lat, lon, bbox)

  }
  /**
   * This method transforms a certain geospatial point within the
   * boundaries of a specific bounding box into pixel coordinates.
   */
  private def latlon2Pixel(
      /* The geospatial coordinates of a point */
      lat:Double, lon:Double,
      /* The geospatial bounding box */
      bbox:BBox): Seq[Int] = {

      /* Extract bounding box */
      val minLon = bbox.minLon
      val maxLon = bbox.maxLon

      val minLat = bbox.minLat
      val maxLat = bbox.maxLat

      val x = math.round(((lon - minLon) / (maxLon - minLon)) * width)
      val y = math.round(((lat - minLat) / (maxLat - minLat)) * height)

      /**
       * We expect that the computes coordinates are
       * non-negative numbers. Negative numbers indicate
       * that the provided point is outside the boundaries
       */
      if (x < 0 || x > width || y < 0 || y > height) return null
      Seq(x.toInt, y.toInt)

  }

  def writeMat(mat:Mat, path:String):Unit = {
    /**
     * STEP #1: Extract path extension
     */
    val tokens = path.split("\\/")
    val fname = tokens.last

    val extension = fname.split("\\.").last.toLowerCase
    /**
     * STEP #2: Transform matrix into byte array
     * and write to file system
     */
    val matOfByte = new MatOfByte()
    Imgcodecs.imencode(s".$extension", mat, matOfByte)

    val bytes = matOfByte.toArray
    save(bytes, extension, path)

  }

  private def save(image:Array[Byte], ext:String, path:String):Unit = {

    val bais = new ByteArrayInputStream(image)
    val bufferedImage = ImageIO.read(bais)

    val target = Paths.get(path)
    ImageIO.write(bufferedImage, ext, target.toFile)

  }

}
