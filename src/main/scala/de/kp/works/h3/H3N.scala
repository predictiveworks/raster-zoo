package de.kp.works.h3
/*
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

import com.uber.h3core._
import com.uber.h3core.util.GeoCoord
import geotrellis.vector.GeomFactory
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.locationtech.jts.geom.{Coordinate, Geometry => JtsGeometry}

import java.lang
import java.util.{List => JList}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Make H3 serializable to enable usage
 * within Apache Spark SQL UDFs
 */
object H3 extends Serializable {
  val instance:H3Core = H3Core.newInstance()
}
/**
 * Scaling spatial operations with H3 is essentially a
 * two step process:
 *
 * The first step is to compute an H3 index for each
 * feature (points, polygons, ...) defined as UDF.
 *
 * The second step is to use these indices for spatial
 * operations such as spatial join (point in polygon,
 * k-nearest neighbors, etc).
 */
object H3Utils extends Serializable {

  def resolutionToM2(res:Int):Double = {
    H3.instance.hexArea(res, AreaUnit.m2)
  }

  /**
   * Indexes the location at the specified resolution,
   * returning the index of the cell containing this
   * location.
   */
  def coordinate_to_H3(resolution:Int):UserDefinedFunction =
    udf((coordinate:Coordinate) => coordinateToH3(coordinate, resolution))

  def coordinateToH3(coordinate:Coordinate, resolution:Int): Long = {
    val (lon, lat) = (coordinate.x, coordinate.y)
    H3.instance.geoToH3(lat, lon, resolution)
  }

  /**
   * This method computes the hexagon cell boundary
   * in latitude, longitude coordinates. This method
   * can be used to assign these hexagons to each tile
   * or to the entire raster image.
   */
  def H3_to_boundaryJTS: UserDefinedFunction =
    udf((index:Long) => H3ToBoundaryJts(index))

  /**
   * This is the explicit variant of the above method
   * that can be used outside a dataframe context.
   */
  def H3ToBoundaryJts(index:Long): JtsGeometry = {
    val coordinates = H3.instance.h3ToGeoBoundary(index)
      .map(coord => new Coordinate(coord.lng, coord.lat)).toArray

    val geom = {
      val line = GeomFactory.factory.createLineString(coordinates)
      if (line.getNumPoints >= 4 && line.isClosed)
        GeomFactory.factory.createPolygon(line.getCoordinateSequence)

      else
        line
    }
    geom
  }
  /**
   * This method leverages the envelope of a provided
   * geometry and computes the hexagon indexes that
   * cover the geometry with respect to the resolution.
   */
  def envelopeJts_to_H3(resolution:Int):UserDefinedFunction =
    udf((geometry:JtsGeometry) => envelopeJtsToH3(geometry, resolution))

  def envelopeJtsToH3(geometry:JtsGeometry, resolution:Int): List[lang.Long] = {
    /*
     * This method specifies a polygon without holes
     */
    val holes:List[JList[GeoCoord]] = List()

    val envelope = geometry.getEnvelope
    val coordinates = envelope.getCoordinates
    /*
     * Boundaries can be specified with start
     * and end point are the same.
     */
    val points = coordinates.map(coordinate => {

      val lat = coordinate.getY
      val lon = coordinate.getX

      new GeoCoord(lat, lon)

    }).toList

    H3.instance.polyfill(points, holes, resolution).toList

  }

  def multigonJts_to_H3(resolution:Int):UserDefinedFunction =
    udf((geometry: JtsGeometry) => multigonJtsToH3(geometry, resolution))

  def multigonJtsToH3(geometry: JtsGeometry, resolution:Int): List[lang.Long] = {

    var points: List[GeoCoord] = List()
    var holes: List[JList[GeoCoord]] = List()

    if (geometry.getGeometryType == "MultiPolygon") {

      val numGeometries = geometry.getNumGeometries
      if (numGeometries > 0) {
        points = List(
          geometry
            .getGeometryN(0)
            .getCoordinates
            .toList
            .map(coord => new GeoCoord(coord.y, coord.x)): _*)
      }

      if (numGeometries > 1) {
        holes = (1 until numGeometries).toList.map(n => {
          List(
            geometry
              .getGeometryN(n)
              .getCoordinates
              .toList
              .map(coord => new GeoCoord(coord.y, coord.x)): _*).asJava
        })
      }
    }

    H3.instance.polyfill(points, holes.asJava, resolution).toList

  }

  def polygonJts_to_H3(resolution:Int):UserDefinedFunction =
    udf((geometry: JtsGeometry) => polygonJtsToH3(geometry, resolution))

  def polygonJtsToH3(geometry: JtsGeometry, resolution:Int):List[lang.Long] = {

    var points: List[GeoCoord] = List()
    val holes: List[JList[GeoCoord]] = List()

    if (geometry.getGeometryType == "Polygon") {
      points = List(
        geometry
          .getCoordinates
          .toList
          .map(coord => new GeoCoord(coord.y, coord.x)): _*)
    }

    H3.instance.polyfill(points, holes.asJava, resolution).toList

  }

}