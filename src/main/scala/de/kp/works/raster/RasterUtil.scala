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
 * The definition of the bounding box is compliant
 * with the one provided by OSM
 */
case class BBox(minLon:Double, minLat:Double, maxLon:Double, maxLat:Double)

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
   * the width and height of the respective pixel
   */
  def dimensions_col(rasterCol:String): TypedColumn[Any, Dimensions[Int]] =
    rf_dimensions(col(rasterCol))

}

object RasterUtil extends Serializable {

  val boundingBoxColName = "bbox"
  private val geometryColName = "_geometry"

  /**
   * Uber's Hexagon Resolution Table
   * -------------------------------
   *
   * Level  | Average Hexagon Area (km2)
   * 0	    | 4,250,546.8477000
   * 1	    | 607,220.9782429
   * 2	    | 86,745.8540347
   * 3	    | 12,392.2648621
   * 4	    | 1,770.3235517
   * 5	    | 252.9033645
   * 6	    | 36.1290521
   * 7	    | 5.1612932
   * 8	    | 0.7373276
   * 9	    | 0.1053325
   * 10	    | 0.0150475
   * 11	    | 0.0021496
   * 12	    | 0.0003071
   * 13	    | 0.0000439
   * 14	    | 0.0000063
   * 15	    | 0.0000009
   */
  private val resolutionTable = Map(
    0  -> 4250546.8477000,
    1  -> 607220.9782429,
    2  -> 86745.8540347,
    3  -> 12392.2648621,
    4  -> 1770.3235517,
    5  -> 252.9033645,
    6  -> 36.1290521,
    7  -> 5.1612932,
    8  -> 737327.6,
    9  -> 105332.5,
    10 -> 15047.5,
    11 -> 2149.6,
    12 -> 307.1,
    13 -> 43.9,
    14 -> 6.3,
    15 -> 0.9
  )

  /**
   * This method computes the bounding box for each tile
   * and assigns a column `box` to the rasterframe.
   */
  def tileBBox(rasterframe:DataFrame, rasterCol:String):DataFrame = {

    rasterframe
      .withColumn(geometryColName, Columns.geometry_col(rasterCol))
      .withColumn(boundingBoxColName, boundingBox(col(geometryColName)))
      .drop(geometryColName)

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
  def computeResolution(rasterframe:DataFrame, rasterCol:String):DataFrame = {

    val crs_col = rf_crs(col(rasterCol))

    rasterframe
      .withColumn("_units",     crsUnits(crs_col))
      .withColumn("_extent",    Columns.extent_col(rasterCol))
      .withColumn("resolution", resolution(resolutionTable)(col("_extent"),col("_units")))
      .drop("_extent")

  }

  private def resolution(table:Map[Int, Double]):UserDefinedFunction = udf((extent:Row, units:String) => {
    /*
     * The current implementation expects `metres`
     * as effective unit
     */
    val xMin = extent.getAs[Double]("xmin")
    val xMax = extent.getAs[Double]("xmax")

    val width = xMax - xMin

    val yMin = extent.getAs[Double]("ymin")
    val yMax = extent.getAs[Double]("ymax")

    val height = yMax - yMin
    val area = width * height

    units match {
      case "m" =>
        /*
         * Convert into km2 to enable comparison with
         * Uber's resolution table
         */
        val km2 = area / (1000 * 1000)
        /*
         * Check which of Uber's average hexagon
         * area covers the tile area. To this end,
         * determine the last hexagon area that is
         * larger than the time area
         */
        val rseq = table.filter{ case(_, hex) =>
          (km2 / hex) <= 1D
        }.toSeq

        if (rseq.isEmpty)
          -1

        else {
          rseq.maxBy(_._1)._1

        }
      case _ => throw new Exception(s"Unit `$units` is not supported.")
    }

  })
  /**
   * This UDF transforms a [Geometry] into a geographic
   * bounding box. The format is compliant with OSM.
   */
  private def boundingBox:UserDefinedFunction = udf((polygon:JtsGeometry) => {

    val boundary = polygon.getBoundary
    val coordinates = boundary.getCoordinates

    val size = coordinates.size
    /*
     * We expect a closed polygon where the last
     * coordinate is equal to the first one
     */
    val lons = Array.fill[Double](size)(0D)
    val lats = Array.fill[Double](size)(0D)

    (0 until size).foreach(i => {
      /*
       * The `x` coordinate refers to the `longitude`
       * and the `y` coordinate is the `latitude`.
       */
      lons(i) = coordinates(i).getX
      lats(i) = coordinates(i).getY

    })

    val minLon = lons.min
    val maxLon = lons.max

    val minLat = lats.min
    val maxLat = lats.max

    BBox(minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat)

  })

  /**
   * This UDF transforms a certain geospatial point within the
   * boundaries of a specific geometry (which represents a raster
   * tile).
   */
  def latlon2Pixel:UserDefinedFunction = udf(
    (dimensions:Row, geometry:JtsGeometry, point:JtsGeometry) => {
    /*
     * DIMENSIONS
     */
    val width = dimensions.getAs[Int]("cols")
    val height = dimensions.getAs[Int]("rows")
    /*
     * GEOMETRY
     */
    val boundary = geometry.getBoundary
    val coordinates = boundary.getCoordinates

    val size = coordinates.size
    /*
     * We expect a closed polygon where the last
     * coordinate is equal to the first one
     */
    val lons = Array.fill[Double](size)(0D)
    val lats = Array.fill[Double](size)(0D)

    (0 until size).foreach(i => {
      /*
       * The `x` coordinate refers to the `longitude`
       * and the `y` coordinate is the `latitude`.
       */
      lons(i) = coordinates(i).getX
      lats(i) = coordinates(i).getY

    })

    val minLon = lons.min
    val maxLon = lons.max

    val minLat = lats.min
    val maxLat = lats.max
    /*
     * POINT
     */
    val lon = point.getCoordinate.getX
    val lat = point.getCoordinate.getY

    val x = math.round(((lon - minLon) / (maxLon - minLon)) * width)
    val y = math.round(((lat - minLat) / (maxLat - minLat)) * height)

    /**
     * We expect that the computes coordinates are
     * non-negative numbers. Negative numbers indicate
     * that the provided point is outside the boundaries
     */
    if (x < 0 || x > width || y < 0 || y > height) return null

    Seq(x, y)

  })

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

