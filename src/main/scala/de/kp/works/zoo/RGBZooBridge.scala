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

import org.apache.spark.rdd.RDD

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.tensor.Tensor

import scala.collection.mutable

case class Composite(height:Int, width:Int, channels:Int, values:Seq[Float])

case class Label(height:Int, width:Int, channels:Int, values:Seq[Float])

object Composer extends Serializable {
  /**
   * This method expects that each tile within a row
   * has the same dimensions and channels
   */
  def buildComposite(colnames:Seq[String]): UserDefinedFunction = udf((row:Row) => {
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
  /*
   * This method transforms a label tile
   */
  def buildLabel(colname:String):UserDefinedFunction = udf((row:Row) => {
    /*
     * Extract tile that refers to the provided colname;
     * note, the row contains more than the `tile` field
     */
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

    val channels = 1
    Label(height, width, channels, values)

  })
}

/**
 * This class transforms (optionally) labeled RGB tiles
 * into an Analytics-Zoo compliant RDD that is ready to
 * train deep learning models.
 *
 * The rasterframe must be indexed as this the prerequisite
 * to re-transform and assign predicted labels to provided
 * imagery data.
 */
class RGBZooBridge extends Serializable {

  private val defaultColName = "proj_raster"
  /*
   * Names for tile columns that contain tiles
   * of the respective color; these columns are
   * transformed into a composite Array[Float].
   *
   * This composite represents the features for
   * supervised deep learning.
   */
  private var redColName:String   = defaultColName
  private var greenColName:String = defaultColName
  private var blueColName:String  = defaultColName

  private var hasLabel:Boolean = true
  private var labelColName:String  = defaultColName

  private var indexColName:String = ""
  /*
   * The name of an internal column that holds the
   * composite representation of an RGB *.png images
   */
  private val compositeColName:String = "composite"

  def setRedCol(name:String):RGBZooBridge = {
    redColName = name
    this
  }

  def setGreenCol(name:String):RGBZooBridge = {
    greenColName = name
    this
  }

  def setBlueCol(name:String):RGBZooBridge = {
    blueColName = name
    this
  }

  def setLabelCol(name:String):RGBZooBridge = {
    labelColName = name
    this
  }

  def setIndexCol(name:String):RGBZooBridge = {
    indexColName = name
    this
  }

  def setHasLabel(value:Boolean):RGBZooBridge = {
    hasLabel = value
    this
  }

  private def validateSchema(rasterframe:DataFrame):Boolean = {

    if (indexColName.isEmpty)
      throw new Exception("The provided dataframe must be indexed to enable re-transformation.")

    val fieldNames = rasterframe.schema.fieldNames

    if (!fieldNames.contains(indexColName))
      throw new Exception("The provided dataframe must be indexed to enable re-transformation.")

    if (!fieldNames.contains(redColName))
      return false

    if (!fieldNames.contains(greenColName))
      return false

    if (!fieldNames.contains(blueColName))
      return false

    if (hasLabel) {

      if (!fieldNames.contains(labelColName))
        return false

    }

    true

  }
  /**
   * NOTE: The functionality provided by this method
   * is a combination of RasterFrame's tile support
   * and Analytics Zoo.
   */
  def transform(rasterframe:DataFrame):RDD[(Long, Sample[Float])] = {

    if (!validateSchema(rasterframe))
      return null

    val red_col   = col(redColName)
    val green_col = col(greenColName)
    val blue_col  = col(blueColName)

    val colnames = List(redColName, greenColName, blueColName)

    val columns = List(red_col, green_col, blue_col)
    val colstruct = struct(columns: _*)

    val composed = rasterframe
      .withColumn(compositeColName,
        Composer.buildComposite(colnames)(colstruct))

    if (!hasLabel) {

      val target = composed.select(indexColName, compositeColName)

      val sc = target.sparkSession.sparkContext
      val colNames = sc.broadcast(indexColName,compositeColName)

      val rdd = target.rdd.map(row => {

        val indexName = colNames.value._1
        val featuresName = colNames.value._2

        val index = row.getAs[Long](indexName)
        val composite = row.getAs[Row](featuresName)

        val width = composite.getAs[Int]("width")
        val height = composite.getAs[Int]("height")

        val channels = composite.getAs[Int]("channels")
        val values = composite.getAs[mutable.WrappedArray[Float]]("values")
        /*
         * IMPORTANT: At this stage, we expect that the shape
         * of all features is the same. This implies, that the
         * user is responsible to pre-process the provided RGB
         * channel tiles.
         */
        val shape = Array(height, width, channels)

        val tensor = Tensor[Float](values.toArray, shape)
        val sample = Sample[Float](tensor)

        (index, sample)

      })

      rdd

    } else {

      val target = composed
        .withColumn(labelColName,
          Composer.buildLabel(labelColName)(col(labelColName)))
        .select(indexColName, compositeColName, labelColName)

      val sc = target.sparkSession.sparkContext
      val colNames = sc.broadcast(indexColName, compositeColName, labelColName)

      val rdd = target.rdd.map(row => {

        val indexName = colNames.value._1

        val index = row.getAs[Long](indexName)
        val features = {

          val featuresName = colNames.value._2
          val composite = row.getAs[Row](featuresName)

          val width = composite.getAs[Int]("width")
          val height = composite.getAs[Int]("height")

          val channels = composite.getAs[Int]("channels")
          val values = composite.getAs[mutable.WrappedArray[Float]]("values")
          /*
           * IMPORTANT: At this stage, we expect that the shape
           * of all features is the same. This implies, that the
           * user is responsible to pre-process the provided RGB
           * channel tiles.
           */
          val shape = Array(height, width, channels)
          val tensor = Tensor[Float](values.toArray, shape)

          tensor

        }
        val label = {

          val labelName = colNames.value._3
          val composite = row.getAs[Row](labelName)

          val width = composite.getAs[Int]("width")
          val height = composite.getAs[Int]("height")

          val channels = composite.getAs[Int]("channels")
          val values = composite.getAs[mutable.WrappedArray[Float]]("values")
          /*
           * IMPORTANT: At this stage, we expect that the shape
           * of all labels is the same. This implies, that the
           * user is responsible to pre-process the provided tiles.
           */
          val shape = Array(height, width, channels)
          val tensor = Tensor[Float](values.toArray, shape)

          tensor

        }

        val sample = Sample[Float](features, label)
        (index, sample)

      })

      rdd

    }

  }

}