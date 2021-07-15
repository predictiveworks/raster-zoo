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

import de.kp.works.geom.model.BBox
import de.kp.works.spark.UDF
import geotrellis.proj4.CRS
import geotrellis.raster.Dimensions
import geotrellis.vector.Extent
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, TypedColumn}
import org.locationtech.jts.geom.{Geometry => JtsGeometry}
import org.locationtech.rasterframes.{rf_crs, rf_dimensions, rf_extent, rf_geometry, rf_render_png, st_centroid, st_reproject}

import java.io.ByteArrayInputStream
import java.nio.file.Paths
import javax.imageio.ImageIO

/**
 * The specification of the Coordinate Reference System
 * that can be extracted from a raster tile.
 */
case class Crs(proj:String, zone:String, datum:String, units:String)

/**
 * A collection of dataframe column helpers for multiple
 * geo transformations.
 */
object Columns extends Serializable {

  val WGS84_ELLIPSOID = "EPSG:4326"
  /**
  * Derived columns for CRS (Coordinate Reference System)
  * and Geometry information
  */
  def crs_col(rasterCol:String): TypedColumn[Any, CRS] =
    rf_crs(col(rasterCol))

  def geom_col(rasterCol:String): TypedColumn[Any, JtsGeometry] =
    rf_geometry(col(rasterCol))
  /**
   * The centroid column specifies the Longitude | Latitude
   * description of the Tile centroid.
   */
  def centroid_col(rasterCol:String): TypedColumn[Any, JtsGeometry] = {

    val crsCol  = crs_col(rasterCol)
    val geomCol = geom_col(rasterCol)

    st_reproject(st_centroid(geomCol), crsCol, lit(WGS84_ELLIPSOID))

  }

  def extent_col(rasterCol:String): TypedColumn[Any, Extent] = {
    rf_extent(col(rasterCol))
  }

  def geometry_col(rasterCol:String): TypedColumn[Any, JtsGeometry] = {

    val crsCol  = crs_col(rasterCol)
    val geomCol = geom_col(rasterCol)

    st_reproject(geomCol, crsCol, lit(WGS84_ELLIPSOID))

  }
  /**
   * The method `rf_dimensions` determines the tile size,
   * i.e. the number of (cols, rows) in a tile. This is
   * the width and height of the respective tile.
   */
  def dimensions_col(rasterCol:String): TypedColumn[Any, Dimensions[Int]] =
    rf_dimensions(col(rasterCol))

}

object RasterUtil extends RasterParams with Serializable {

  val boundingBoxColName = "bbox"
  val countColName = "count"

  /**
   * This method computes the bounding box for each tile
   * and assigns a column `bbox` to the rasterframe.
   */
  def tileBBox(rasterframe:DataFrame, rasterCol:String):DataFrame = {

    rasterframe
      .withColumn(geometryColName, Columns.geometry_col(rasterCol))
      .withColumn(boundingBoxColName, UDF.boundingBox(col(geometryColName)))
      .drop(geometryColName)

  }

  /**
   * This method computes the width and height
   * for each tile and assigns the columns `width`
   * and `height`.
   */
  def tileDimension(rasterframe:DataFrame, rasterCol:String):DataFrame = {

    rasterframe
      .withColumn(dimensionColName, Columns.dimensions_col(rasterCol))
      .withColumn(widthColName,  col(dimensionColName).getItem("cols"))
      .withColumn(heightColName, col(dimensionColName).getItem("rows"))
      .drop(dimensionColName)

  }

  /**
   * This method computes the statistics of the
   * tile dimensions, width and height.
   *
   * Note, this method expects a dataframe with
   * columns `width` and `height`.
   *
   * This statistics can be used to control the
   * data preprocessing stage with respect to
   * Analytics Zoo.
   *
   * Deep learning requires a training and label
   * dataset where training datapoint and each
   * label point has the same dimension.
   */
  def tileStat(rasterframe:DataFrame):DataFrame = {

    val fieldNames = rasterframe.schema.fieldNames
    if (!fieldNames.contains(widthColName))
      throw new Exception(s"The rasterframe does not contain column `$widthColName`.")

    if (!fieldNames.contains(heightColName))
      throw new Exception(s"The rasterframe does not contain column `$heightColName`.")

    rasterframe
      .groupBy(col(widthColName), col(heightColName))
      .agg(count(indexColName).as(countColName))
  }
  /**
   * This is a fast method to compute the entire
   * bounding box of all tiles of the rasterframe.
   *
   * Leveraging a rasterframe as starting point is
   * significantly faster than using GeoTrellis API
   * for single band GeoTIFFs.
   */
  def computeBBox(rasterframe:DataFrame, rasterCol:String):BBox = {

    val annotated = tileBBox(rasterframe, rasterCol).select(boundingBoxColName)
      .withColumn("minLon", col(boundingBoxColName).getItem("minLon"))
      .withColumn("maxLon", col(boundingBoxColName).getItem("maxLon"))
      .withColumn("minLat", col(boundingBoxColName).getItem("minLat"))
      .withColumn("maxLat", col(boundingBoxColName).getItem("maxLat"))
      .drop(boundingBoxColName)

    val aggCols = Seq(
      min("minLon"), max("maxLon"),
      min("minLat"), max("maxLat"))

    val aggregated = annotated.agg(aggCols.head, aggCols.tail: _*)
    /*
     * +------------------+------------------+-----------------+-----------------+
     * |       min(minLon)|       max(maxLon)|      min(minLat)|      max(maxLat)|
     * +------------------+------------------+-----------------+-----------------+
     */
    val bbox = aggregated.head

    val minLon = bbox.getAs[Double]("min(minLon)")
    val maxLon = bbox.getAs[Double]("max(maxLon)")

    val minLat = bbox.getAs[Double]("min(minLat)")
    val maxLat = bbox.getAs[Double]("max(maxLat)")

    BBox(minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat)

  }

  /**
   * This method computes the resolution of each tile
   * with respect to the H3 hexagon geospatial indexing
   * system.
   */
  def computeResolution(rasterframe:DataFrame, rasterCol:String):DataFrame = {

    val crs_col = rf_crs(col(rasterCol))

    rasterframe
      .withColumn(unitsColName,  crsUnits(crs_col))
      .withColumn(extentColName, Columns.extent_col(rasterCol))
      .withColumn(resolutionColName, UDF.findResolution(col(extentColName),col(unitsColName)))
      .drop(extentColName, unitsColName)

  }

  def crsUnits:UserDefinedFunction = udf((row:Row) => {

    val rawStr = row.getAs[String]("crsProj4")

    var proj:String = ""
    var zone:String = ""

    var datum:String = ""
    var units:String = ""

    val tokens = rawStr.split("\\s+")
    tokens.foreach(token => {
      val cleaned = token.replace("+", "")
      if (cleaned.startsWith("proj=")) {
        proj = token.split("proj=")(1).trim
      }
      if (cleaned.startsWith("zone=")) {
        zone = token.split("zone=")(1).trim
      }
      if (cleaned.startsWith("datum=")) {
        datum = token.split("datum=")(1).trim
      }
      if (cleaned.startsWith("units=")) {
        units = token.split("units=")(1).trim
      }
    })

    units

  })
  /**
   * Coordinate Reference System
   *
   * This method expects that the CRS is the same for
   * all tiles provided, and therefore picks the first
   * one to extract the CRS.
   */
  def extractCRS(rasterFrame:DataFrame, rasterCol:String):Crs = {

    val crs_col = rf_crs(col(rasterCol))
    val crs = rasterFrame.select(crs_col).limit(1).collect.head

    val raw_str = crs.toProj4String

    var proj:String = ""
    var zone:String = ""

    var datum:String = ""
    var units:String = ""

    val tokens = raw_str.split("\\s+")
    tokens.foreach(token => {
      val cleaned = token.replace("+", "")
      if (cleaned.startsWith("proj=")) {
        proj = token.split("proj=")(1).trim
      }
      if (cleaned.startsWith("zone=")) {
        zone = token.split("zone=")(1).trim
      }
      if (cleaned.startsWith("datum=")) {
        datum = token.split("datum=")(1).trim
      }
      if (cleaned.startsWith("units=")) {
        units = token.split("units=")(1).trim
      }
    })

    Crs(proj, zone, datum, units)

  }

  def render2PNG(dataframe:DataFrame, red:String, green:String, blue:String):DataFrame = {

    /* RGB requires 3 columns for red, green and blue */
    val red_col   = col(red)
    val green_col = col(green)
    val blue_col  = col(blue)

    val renderer = rf_render_png(red_col, green_col, blue_col)
    val rendered = dataframe
      .withColumn("png", renderer).select("png").cache
    /*
     * The output contains a single column `png`
     * which holds the tile images as byte array
     */
    rendered

  }

  def savePNG(image:Array[Byte], path:String):Unit = {

    val bais = new ByteArrayInputStream(image)
    val bufferedImage = ImageIO.read(bais)
    /*
     * Note: The buffered image can be used to annotate
     * or enrich the image:
     *
     * val g = bufferedImage.createGraphics
     * g.setFont(new Font("TimesRoman", Font.BOLD, 30))
     * g.setColor(Color.WHITE)
     * g.drawString("Hello World", 100, 100)
     *
     */
    val target = Paths.get(path)
    ImageIO.write(bufferedImage, "png", target.toFile)

  }

 }

