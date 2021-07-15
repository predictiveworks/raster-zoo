package de.kp.works.spark

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
import de.kp.works.geom.functions._
import de.kp.works.geom.model.BBox
import de.kp.works.h3.H3Utils
import de.kp.works.osm.{GeometryUtils, Member, Node}
import de.kp.works.vectorpipe.functions.osm
import geotrellis.vector.GeomFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.locationtech.jts.geom.{Coordinate, Geometry => JtsGeometry}

import scala.collection.mutable

/**
 * A geometry describes an ordered sequence of (lat, lon) points,
 * that is either linestring (open) or polygon (closed).
 */
case class Geometry(
   uuid:String, snode:Long, enode:Long, path:Seq[Seq[Double]], length:Int, geometry:String)

case class Segment(geometries:Seq[Geometry], `type`:String)

object UDF extends Serializable {

  /**
   * This UDF transforms a [Geometry] into a geographic
   * bounding box. The format is compliant with OSM.
   */
  def boundingBox:UserDefinedFunction =
    udf((geometry:JtsGeometry) => geometryToBBox(geometry))

  def get_envelope:UserDefinedFunction =
    udf((geometry:JtsGeometry) => envelopeToBoundary(geometry))

  /**
   * This method transforms the extent of a tile
   * in square metres. That resolution is determined
   * where the area of the associated hexagon has
   * the minimal difference from the area of the
   * extent.
   */
  def findResolution:UserDefinedFunction =
    udf((extent:Row, units:String) => {
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
           * Compute the minimal difference between
           * the area and the respective hexagon area
           */
          val ds = (0 until 15).map(r => {
            val d = math.abs(area - H3Utils.resolutionToM2(r))
            (r, d)
          })

          val res = ds.minBy(_._2)._1
          res

        case _ => throw new Exception(s"Unit `$units` is not supported.")
      }

    })

  /**
   * This UDF transforms a certain geospatial point within the
   * boundaries of a specific geometry into a Pixel point.
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

  /**
   * This method transforms an Apache Spark DataFrame
   * Row (back) into the Geometry Format
   */
  def row2Geometry(row:Row):Geometry = {

    val uuid = row.getAs[String]("uuid")

    val snode = row.getAs[Long]("snode")
    val enode = row.getAs[Long]("enode")

    val geometry = if (snode == enode) "polygon" else "linestring"

    val path = row.getAs[mutable.WrappedArray[mutable.WrappedArray[Double]]]("path")
    val length = row.getAs[Int]("length")

    Geometry(uuid, snode, enode, path, length, geometry)
  }

  /**
   * This method transforms a `Geometry` column into a
   * bounding box
   */
  def geometry2BBox:UserDefinedFunction =
    udf((geometry:Row) => {

      val path = geometry
        .getAs[mutable.WrappedArray[mutable.WrappedArray[Double]]]("path")

      val lats = path.map(point => point(0))
      val lons = path.map(point => point(1))

      val minLat = lats.min
      val maxLat = lats.max

      val minLon = lons.min
      val maxLon = lons.max

      BBox(minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat)

    })

  /**
   * A filter method to restrict the content
   * of OSM nodes to the provided bounding box
   */
  def limit2bbox(bbox:BBox): UserDefinedFunction = udf((lat:Double, lon:Double) => {
    if (
      bbox.minLon <= lon &&
        lon <= bbox.maxLon &&
        bbox.minLat <= lat &&
        lat <= bbox.maxLat) true else false
  })
  /**
   * This method extracts the members that refer
   * to a certain relation and prepares for exploding
   */
  def extractMembers:UserDefinedFunction =
    udf((members:mutable.WrappedArray[Row]) => {
      members.map(member => {

        val mid = member.getAs[Long]("id")
        val mtype = new String(member.getAs[Array[Byte]]("type"))

        var mrole = new String(member.getAs[Array[Byte]]("role"))
        if (mrole.isEmpty) mrole = "unknown"

        Member(mid, mrole, mtype)

      })
    })
  /**
   * This method transforms the nodes that refer
   * to a certain way.
   */
  def extractNodes:UserDefinedFunction =
    udf((nodes:mutable.WrappedArray[Row]) => {
      nodes.map(node => {
        val nix = node.getAs[Int]("index")
        val nid = node.getAs[Long]("nodeId")

        Node(nix, nid)
      })
  })
  /**
   * This method transforms the ordered latitude-longitude
   * pairs of a specific way into a geospatial polygon.
   */
  def buildGeometry:UserDefinedFunction =
    udf((nodes:mutable.WrappedArray[Row]) => {
      val data = nodes.map(node => {

        val node_ix = node.getAs[Int]("wnode_ix")
        val node_id = node.getAs[Long]("wnode_id")

        val latitude = node.getAs[Double]("latitude")
        val longitude = node.getAs[Double]("longitude")

        (node_ix, node_id, latitude, longitude)

      })
      .sortBy(_._1)
      .map { case (_, id, lat, lon) => (id, Seq(lat, lon)) }

      val snode = data.head._1
      val enode = data.last._1

      val geometry = if (snode == enode) "polygon" else "linestring"

      val path = data.map{ case(_, point) => point}

      val uuid = java.util.UUID.randomUUID.toString
      Geometry(uuid, snode, enode, path, path.length, geometry)

    })

  /**
   * This method transforms the unordered sequence of
   * nodes and its associated coordinates into a JTS
   * compliant geometry.
   *
   * It is best used with enriched OSM ways. This kind
   * of format also enables a simple transformation into
   * GeoJSON.
   */
  def buildJtsGeometry:UserDefinedFunction =
    udf((nodes:mutable.WrappedArray[Row], isArea:Boolean) => {

      val values = nodes.map(node => {

        val node_ix = node.getAs[Int]("wnode_ix")

        val latitude = node.getAs[Double]("latitude")
        val longitude = node.getAs[Double]("longitude")

        (node_ix, latitude, longitude)

      })
      .sortBy(_._1)
       /*
        * IMPORTANT:
        *
        * The JtsGeometry is based on LONGITUDE, LATITUDE
        * pairs, and to be in sync with all other operations
        * with this datatype, the ordering of the coordinates
        * is changed here.
        */
      .map { case (_, lat, lon) => Seq(lon, lat) }
      .toVector

      val geom = values match {
        /* No coordinates provided */
        case coords if coords.isEmpty =>
          Some(GeomFactory.factory.createLineString(Array.empty[Coordinate]))
        /* Some of the coordinates are empty; this is invalid */
        case coords if coords.exists(Option(_).isEmpty) =>
          None
        /* Some of the coordinates are invalid */
        case coords if coords.exists(_.exists(_.isNaN)) =>
          None
        /* 1 pair of coordinates provided */
        case coords if coords.length == 1 =>
          Some(GeomFactory.factory.createPoint(new Coordinate(coords.head.head, coords.head.last)))

        case coords =>
          val coordinates = coords.map(xy => new Coordinate(xy.head, xy.last)).toArray
          val line = GeomFactory.factory.createLineString(coordinates)

          if (isArea && line.getNumPoints >= 4 && line.isClosed)
            Some(GeomFactory.factory.createPolygon(line.getCoordinateSequence))
          else
            Some(line)
      }

      val geometry = geom match {
        case Some(g) if g.isValid => g
        case _ => null
      }

      geometry

    })

  def buildSegments:UserDefinedFunction = {
    udf((rows:mutable.WrappedArray[Row]) => {
      /**
       * STEP #1: Separate the polygons by its specified `member_role`.
       * This is a data preparation step as only polygons are concatenated
       * that refer to the same `member_role`.
       */
      val roles = mutable.HashMap.empty[String, mutable.ArrayBuffer[Geometry]]
      rows.foreach(row => {

        val role = row.getAs[String]("member_role")
        val polygon = row2Geometry(row.getAs[Row]("polygon"))

        val key = if (role.trim.isEmpty) "unknown" else role.trim
        if (!roles.contains(key))
          roles += key -> mutable.ArrayBuffer.empty[Geometry]

        roles(key) += polygon

      })
      /**
       * STEP #2: Concatenate the polygons that refer to the
       * same `member_role`.
       */
      val segments = roles
        .map{ case(role, ways) =>

          if (ways.isEmpty) Segment(ways, role)
          else
            Segment(GeometryUtils.buildSegments(ways), role)
        }
        .filter(segment => segment.geometries.nonEmpty)
        .toSeq

      segments

     })
  }
}
