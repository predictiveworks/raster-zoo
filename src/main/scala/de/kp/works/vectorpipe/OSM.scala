package de.kp.works.vectorpipe
/**
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2011-2017 Azavea [http://www.azavea.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import geotrellis.vector._

import de.kp.works.vectorpipe.functions.osm.removeUninterestingTags
import de.kp.works.vectorpipe.internal._

object OSM {
  /**
    * Convert a raw OSM dataframe into a frame containing JTS geometries for each unique id/changeset.
    *
    * This currently produces Points for nodes containing "interesting" tags, LineStrings and Polygons for ways
    * (according to OSM rules for defining areas), MultiPolygons for multipolygon and boundary relations, and
    * LineStrings / MultiLineStrings for route relations.
    */
  def toGeometry(input: DataFrame, `type`:String): DataFrame = {
    import input.sparkSession.implicits._

    val st_pointToGeom = org.apache.spark.sql.functions.udf { pt: Point => pt.asInstanceOf[Geometry] }

    val elements = input
      .withColumn("tags", removeUninterestingTags('tags))
      /* __MOD__ */
      .withColumn("type", lit(`type`))

    import org.apache.spark.sql.expressions.Window
    @transient val idByVersion = Window.partitionBy('node_id).orderBy('version)

    val test = elements.withColumn("lagged", lag("timestamp", 1).over(idByVersion))
    test.show
    //val nodes = preprocessNodes(elements)
    throw new Exception
//    val nodeGeoms = constructPointGeometries(nodes)  // st_poin
//      .withColumn("minorVersion", lit(0))
//      .withColumn("geom", st_pointToGeom('geom))
//
//    val wayGeoms = reconstructWayGeometries(elements, nodes) // interesting functionality
//    must be adapted to
//
//    val relationGeoms = reconstructRelationGeometries(elements, wayGeoms)
//
//    nodeGeoms
//      .union(wayGeoms.where(size('tags) > 0).drop('geometryChanged))
//      .union(relationGeoms)
  }

  /**
    * Snapshot pre-processed elements.
    *
    * A Time Pin is stuck through a set of elements that have been augmented with a 'validUntil column to identify all
    * that were valid at a specific point in time (i.e. updated before the target timestamp and valid after it).
    *
    * @param df        Elements (including 'validUntil column)
    * @param timestamp Optional timestamp to snapshot at
    * @return DataFrame containing valid elements at timestamp (or now)
    */
  def snapshot(df: DataFrame, timestamp: Timestamp = null): DataFrame = {
    import df.sparkSession.implicits._

    df
      .where(
        'updated <= coalesce(lit(timestamp), current_timestamp)
          and coalesce(lit(timestamp), current_timestamp) < coalesce('validUntil, date_add(current_timestamp, 1)))
  }

  /**
    * Augment geometries with user metadata.
    *
    * When 'changeset is included, user (name and 'uid) metadata is joined from a DataFrame containing changeset
    * metadata.
    *
    * @param geoms      Geometries to augment.
    * @param changesets Changesets DataFrame with user metadata.
    * @return Geometries augmented with user metadata.
    */
  def addUserMetadata(geoms: DataFrame, changesets: DataFrame): DataFrame = {
    import geoms.sparkSession.implicits._

    geoms
      .join(changesets.select('id as 'changeset, 'uid, 'user), Seq("changeset"))
  }

}
