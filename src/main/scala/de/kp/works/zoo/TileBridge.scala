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

import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.zoo.feature.image._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.locationtech.rasterframes.rf_render_png
import org.opencv.imgcodecs.Imgcodecs

/**
 * TODO: Support for labeled images
 */
class TileBridge {

  private val defaultColName = "proj_raster"
  /*
   * Names for tile columns that contain tiles
   * of the respective color
   */
  private var redColName:String   = defaultColName
  private var greenColName:String = defaultColName
  private var blueColName:String  = defaultColName
  /*
   * The name of the column that holds the image uri
   */
  private var uriColName:String = "proj_raster_path"

  /*
   * The name of an internal column that holds the
   * byte representation of an RGB *.png images
   */
  private val bytesColName:String = "_bytes"

  def setRedCol(name:String):TileBridge = {
    redColName = name
    this
  }

  def setGreenCol(name:String):TileBridge = {
    greenColName = name
    this
  }

  def setBlueCol(name:String):TileBridge = {
    blueColName = name
    this
  }

  def setUriCol(name:String):TileBridge = {
    uriColName = name
    this
  }

  /**
   * NOTE: The functionality provided by this method
   * is a combination of RasterFrame's rendering support
   * and Analytics Zoo `NNImageReader`.
   */
  def transform(rasterframe:DataFrame,
                /*
                 * Height after resize, by default is -1
                 * which will not resize the image
                 */
                resizeH:Int = -1,
                /*
                 * Width after resize, by default is -1
                 * which will not resize the image
                 */
                resizeW:Int = -1,
                /*
                 * Specifies the color type of a loaded image,
                 * same as in OpenCV.imread.
                 *
                 * By default is Imgcodecs.CV_LOAD_IMAGE_UNCHANGED.
                 *
                 * > 0 Return a 3-channel color image. In the current
                 *     implementation the alpha channel, if any, is
                 *     stripped from the output image. Use negative value
                 *     if you need the alpha channel.
                 *
                 * = 0 Return a grayscale image.
                 *
                 * < 0 Return the loaded image as is (with alpha channel
                 *   if any).
                 */
                imageCodec: Int = Imgcodecs.CV_LOAD_IMAGE_UNCHANGED):DataFrame = {

    val session = rasterframe.sparkSession
    /**
     * STEP #1: The PNG rendering method of RasterFrames
     * is used to convert each tile into an RGB-compliant
     * Array[Byte]
     */
    val red_col   = col(redColName)
    val green_col = col(greenColName)
    val blue_col  = col(blueColName)

    val toBytes = rf_render_png(red_col, green_col, blue_col)
    val bytesDF = rasterframe
      .withColumn(bytesColName, toBytes)

    /**
     * STEP #2: The `proj_raster_path` and the `bytes`
     * column is used transform both columns into Zoo's
     * ImageFeature
     */
    val images = bytesDF
      .select(uriColName, bytesColName)
      /*
       * As no data type definition is defined for Zoo's
       * `ImageFeature`, the transformation to an RDD is
       * used to convert into an ImageFeature.
       */
      .rdd.map(row => {

        val uri   = row.getAs[String](uriColName)
        val bytes = row.getAs[Array[Byte]](bytesColName)

        ImageFeature(bytes=bytes, uri=uri)

       })
    /**
     * STEP #3: Enrich the ImageFeature with OpenCV (Mat)
     * features
     */
    val imageSet =
      transform2Mat(ImageSet.rdd(images), resizeH, resizeW, imageCodec)
    /**
     * STEP #4: Transform features of enriched ImageFeature
     * into Apache Spark's Row and use image schema to return
     * a single image column
     */
    val rowRDD = imageSet.toDistributed().rdd.map { imf =>
      Row(SchemaBridge.imf2Row(imf))
    }

    session.createDataFrame(rowRDD, SchemaBridge.imageSchema)

  }
  /**
   * This method is extracted from Zoo's `ImageSet`.
   */
  private def transform2Mat(imageSet:ImageSet, resizeH:Int, resizeW:Int, imageCodec:Int):ImageSet = {

    if (resizeW == -1 || resizeH == -1) {
      imageSet -> ImageBytesToMat(imageCodec = imageCodec)

    } else {
      imageSet -> BufferedImageResize(resizeH, resizeW) ->
        ImageBytesToMat(imageCodec = imageCodec)
    }
  }

}