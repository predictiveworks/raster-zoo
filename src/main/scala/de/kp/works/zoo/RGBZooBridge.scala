package de.kp.works.zoo
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

import geotrellis.raster.Tile
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import com.intel.analytics.bigdl.tensor.Tensor
import org.apache.spark.rdd.RDD

import scala.collection.mutable

case class Composite(height:Int, width:Int, channels:Int, values:Seq[Float])

object Composer extends Serializable {

  def compose(colnames:Seq[String]): UserDefinedFunction = udf((row:Row) => {
    /*
     * Extract tiles that refer to the provided colnames
     */
    val tiles = colnames.map(colname => {

      val tile = row.getAs[Row](colname).getAs[Tile]("tile")

      val width  = tile.cols
      val height = tile.rows

      val values = tile.toArray.map(_.toFloat)
      val size = values.length
      /*
       * This implementation supports a single channel
       * for each tile.
       */
      if (width * height != size)
        throw new Exception(s"Only single channel tile are supported yet")

      (width, height, values)
    })
    /*
     * We expect that each tile within the same row has
     * the same dimensions.
     */
    val ws = tiles.map(_._1)
    if (ws.min != ws.max)
      throw new Exception(s"All tiles must have the same width.")

    val hs = tiles.map(_._2)
    if (hs.min != hs.max)
      throw new Exception(s"All tiles must have the same width.")

    /*
     * All tiles have the same dimensions and are merged
     * into a Seq[Seq[Float]]: height x width x channels
     */

    val channels = colnames.size
    val values = tiles.flatMap(_._3)

    val height = hs.head
    val width  = hs.head

    Composite(height, width, channels, values)

  })

}
/**
 * TODO: Support for labeled images
 */
class RGBTileBridge extends Serializable {

  private val defaultColName = "proj_raster"
  /*
   * Names for tile columns that contain tiles
   * of the respective color
   */
  private var redColName:String   = defaultColName
  private var greenColName:String = defaultColName
  private var blueColName:String  = defaultColName

  /*
   * The name of an internal column that holds the
   * composite representation of an RGB *.png images
   */
  private val compositeColName:String = "composite"

  def setRedCol(name:String):RGBTileBridge = {
    redColName = name
    this
  }

  def setGreenCol(name:String):RGBTileBridge = {
    greenColName = name
    this
  }

  def setBlueCol(name:String):RGBTileBridge = {
    blueColName = name
    this
  }

  /**
   * NOTE: The functionality provided by this method
   * is a combination of RasterFrame's tile support
   * and Analytics Zoo.
   */
  def transform(rasterframe:DataFrame):RDD[(Long, Tensor[Float])] = {

    val red_col   = col(redColName)
    val green_col = col(greenColName)
    val blue_col  = col(blueColName)

    val colnames = List(redColName, greenColName, blueColName)

    val columns = List(red_col, green_col, blue_col)
    val colstruct = struct(columns: _*)

    val composed = rasterframe
      .withColumn(compositeColName, Composer.compose(colnames)(colstruct))
      .select("index", compositeColName)

    val rdd = composed.rdd.map(row => {

      val index = row.getAs[Long]("index")
      val composite = row.getAs[Row](compositeColName)

      val width = composite.getAs[Int]("width")
      val height = composite.getAs[Int]("height")

      val channels = composite.getAs[Int]("channels")
      val values = composite.getAs[mutable.WrappedArray[Float]]("values")
      val shape = Array(height, width, channels)

      val tensor = Tensor[Float](values.toArray, shape)

      (index, tensor)
    })

    rdd

  }

}