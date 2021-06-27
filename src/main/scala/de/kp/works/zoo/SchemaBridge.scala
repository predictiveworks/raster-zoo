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

import com.intel.analytics.bigdl.tensor.{Storage, Tensor}
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.bigdl.transform.vision.image.opencv.OpenCVMat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.opencv.core.CvType

object SchemaBridge extends Serializable {

  /**
   * Schema for the image column in a DataFrame. Image data
   * is saved in an array of Bytes.
   */
  val byteSchema: StructType = StructType(
    StructField("origin",    StringType,  nullable = true) ::
    StructField("height",    IntegerType, nullable = false) ::
    StructField("width",     IntegerType, nullable = false) ::
    StructField("nChannels", IntegerType, nullable = false) ::
    // OpenCV-compatible type: CV_8UC3, CV_8UC1 in most cases
    StructField("mode",      IntegerType, nullable = false) ::
    // Bytes in row-wise BGR
    StructField("data",      BinaryType,  nullable = false) :: Nil)

  /**
   * Schema for the image column in a DataFrame. Image data is
   * saved in an array of Floats.
   */
  val floatSchema: StructType = StructType(
    StructField("origin",    StringType,  nullable = true) ::
    StructField("height",    IntegerType, nullable = false) ::
    StructField("width",     IntegerType, nullable = false) ::
    StructField("nChannels", IntegerType, nullable = false) ::
    // OpenCV-compatible type: CV_32FC3 in most cases
    StructField("mode",      IntegerType, nullable = false) ::
    // floats in OpenCV-compatible order: row-wise BGR in most cases
    StructField("data",      new ArrayType(
      FloatType, false), nullable = false) :: Nil)

  /**
   * This schema is based on the `byteSchema` and concentrates
   * an image in a single `image` column
   */
  val imageSchema: StructType =
    StructType(StructField("image", byteSchema, nullable = true) :: Nil)

  /**
   * This method is a copy of the respective method in Analytics-Zoo
   * `NNImageSchema`. It closes the gap to any Zoo-specific deep
   * learning and image processing task.
   */
  def imf2Row(imf: ImageFeature): Row = {

    val (mode, data) = if (imf.contains(ImageFeature.imageTensor)) {

      val floatData = imf(ImageFeature.imageTensor).asInstanceOf[Tensor[Float]].storage().array()
      val cvType = imf.getChannel() match {
        case 1 => CvType.CV_32FC1
        case 3 => CvType.CV_32FC3
        case 4 => CvType.CV_32FC4
        case other => throw new IllegalArgumentException(s"Unsupported number of channels:" +
          s" $other in ${imf.uri()}. Only 1, 3 and 4 are supported.")
      }
      (cvType, floatData)

    } else if (imf.contains(ImageFeature.mat)) {

      val mat = imf.opencvMat()
      val cvType = mat.`type`()
      val bytesData = OpenCVMat.toBytePixels(mat)._1
      (cvType, bytesData)

    } else {
      throw new IllegalArgumentException(s"ImageFeature should have imageTensor or mat.")
    }

    Row(
      imf.uri(),
      imf.getHeight(),
      imf.getWidth(),
      imf.getChannel(),
      mode,
      data
    )
  }
  /**
   * This method is a copy of the respective method in Analytics-Zoo
   * `NNImageSchema`.
   */
  def row2Imf(row: Row): ImageFeature = {

    val (origin, h, w, c) = (row.getString(0), row.getInt(1), row.getInt(2), row.getInt(3))
    val imf = ImageFeature()

    imf.update(ImageFeature.uri, origin)
    imf.update(ImageFeature.size, (h, w, c))

    val storageType = row.getInt(4)
    storageType match {
      case CvType.CV_8UC3 | CvType.CV_8UC1 | CvType.CV_8UC4 =>

        val bytesData = row.getAs[Array[Byte]](5)
        val opencvMat = OpenCVMat.fromPixelsBytes(bytesData, h, w, c)

        imf(ImageFeature.mat) = opencvMat
        imf(ImageFeature.originalSize) = opencvMat.shape()
      case CvType.CV_32FC3 | CvType.CV_32FC1 | CvType.CV_32FC4 =>

        val data = row.getSeq[Float](5).toArray
        val size = Array(h, w, c)

        val tensor = Tensor(Storage(data)).resize(size)
        imf.update(ImageFeature.imageTensor, tensor)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type in imageColumn: $storageType")
    }

    imf
  }

  /**
   * Gets the origin of the image
   */
  def getOrigin(row: Row): String = row.getString(0)

}
