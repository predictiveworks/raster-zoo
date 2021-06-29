package de.kp.works.osm
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
import de.kp.works.raster.BBox
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

case class Geometry(
   uuid:String, snode:Long, enode:Long, path:Seq[Seq[Double]], length:Int, geometry:String)

case class Segment(polygons:Seq[Geometry], `type`:String)

object UDF extends Serializable {
  /**
   * This method evaluates the `tag` column of an OSM-specific
   * dataframe and evaluates whether the provided key matches
   * one of the tag keys. In case of a match, the associated
   * value is returned.
   */
  def keyMatch(key:String): UserDefinedFunction =
    udf((tags: mutable.WrappedArray[Row]) => {

      var value:String = ""
      tags.foreach(tag => {

        val k = new String(tag.getAs[Array[Byte]]("key"))
        val v = new String(tag.getAs[Array[Byte]]("value"))

        if (k ==  key) value = v

    })

    value

  })

  def limit2bbox(bbox:BBox): UserDefinedFunction = udf((lat:Double, lon:Double) => {
    if (
      bbox.minLon <= lon &&
        lon <= bbox.maxLon &&
        bbox.minLat <= lat &&
        lat <= bbox.maxLat) true else false
  })

  /**
   * This method transforms the members that refer
   * to a certain relation
   */
  def extractMembers:UserDefinedFunction =
    udf((members:mutable.WrappedArray[Row]) => {
      members.map(member => {

        val mid = member.getAs[Long]("id")
        val mrole = new String(member.getAs[Array[Byte]]("role"))
        val mtype = new String(member.getAs[Array[Byte]]("type"))

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

        val node_ix = node.getAs[Int]("node_ix")
        val node_id = node.getAs[Long]("node_id")

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

  def buildSegments:UserDefinedFunction = {
    udf((rows:mutable.WrappedArray[Row]) => {
      /**
       * STEP #1: Separate the polygons by its specified `member_role`.
       * This is a data preparation step as only polygons are concatenated
       * that refer to the same `member_role`.
       */

      def row2Polygon(row:Row):Geometry = {

        val uuid = row.getAs[String]("uuid")

        val snode = row.getAs[Long]("snode")
        val enode = row.getAs[Long]("enode")

        val geometry = if (snode == enode) "polygon" else "linestring"

        val path = row.getAs[mutable.WrappedArray[mutable.WrappedArray[Double]]]("path")
        val length = row.getAs[Int]("length")

        Geometry(uuid, snode, enode, path, length, geometry)
      }

      val roles = mutable.HashMap.empty[String, mutable.ArrayBuffer[Geometry]]
      rows.foreach(row => {

        val role = row.getAs[String]("member_role")
        val polygon = row2Polygon(row.getAs[Row]("polygon"))

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
        .filter(segment => segment.polygons.nonEmpty)
        .toSeq

      segments

     })
  }
}
