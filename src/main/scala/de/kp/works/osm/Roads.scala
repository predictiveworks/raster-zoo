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

import de.kp.works.geom.model.BBox
import de.kp.works.osm.functions.key_match
import de.kp.works.spark.UDF
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Roads extends Entities {

  val highway: String = HIGHWAY
  /**
   * This method extracts relations or ways, specified as `highway`,
   * that fall within the provided bounding box.
   */
  def limitedWays2Highways(bbox:BBox):DataFrame = {

    val query = Query(
      key=highway, params=Map.empty[String,String])

    limitWays(bbox, query)

  }
  /**
   * This method transforms OSM relations of type
   * (key) `highway` into connected geospatial
   * geometries.
   *
   * These geometries are distinguished by `relation_id`,
   * highway type, `tags` and `version`.
   */
  def limitedRelations2Highways(bbox:BBox):DataFrame = {

    val relations = loadRelations.drop(DROP_COLS: _*)
    val ways = loadWays.drop(DROP_COLS: _*)

    /**************************************************
     *
     * STAGE #1: Restrict the available relations
     * to those that match the predefined tag key
     *
     **************************************************/

    val highways = relations
      .withColumn(highway, key_match(highway)(col(TAGS)))
      .filter(not(col(highway) === ""))

    /**************************************************
     *
     * STAGE #2: Transform relations and extract and
     * explode members. The result is a dataset that
     * where each member of a relation is specified by
     * its own row.
     *
     **************************************************/
    /*
     * This is the leading dataset for all subsequent
     * joining operations. This implies that the version
     * of a certain relation is maintained.
     */
    val members = buildRelationMembers(highways)

    /**************************************************
     *
     * STAGE #3: Join members with ways to retrieve
     * the way elements that describe a highway.
     *
     * These members are exploded to the node level
     * and are prepared to finally join with nodes
     * to extract the coordinates of each node.
     *
     **************************************************/

    val way_members = buildRelationWays(members, ways)

    /**************************************************
     *
     * STAGE #4: Join members with relations to retrieve
     * the relation elements that describe a highway
     *
     **************************************************/

    val relation_members = buildRelationRelations(members, relations)

    /**************************************************
     *
     * STAGE #5: Join members with ways to retrieve
     * the way elements that describe a highway.
     *
     * These members are exploded to the node level
     * and are prepared to finally join with nodes
     * to extract the coordinates of each node.
     *
     **************************************************/

    val way_relation_members = buildRelationWays(relation_members, ways)

    /**************************************************
     *
     * STAGE #6: Merge both node descriptions and prepare
     * for joining with nodes to retrieve the associated
     * geo coordinates
     *
     **************************************************/

    val nodeset = way_members.union(way_relation_members)

    /**************************************************
     *
     * STAGE #7: Join nodeset with nodes to retrieve
     * the node elements that describe a relation
     *
     **************************************************/

    val limitedNodes = limitNodes(bbox)

    val relation_nodes = nodeset
      .join(limitedNodes, way_members("wnode_id") === limitedNodes("node_id"), "inner")
      .drop("node_id")

    /**************************************************
     *
     * STAGE #8: Reorganize relations and collect all
     * coordinates (lat & lon), member_role and way_id
     * that refer to a certain relation.
     *
     * As a result, every way (way_id) of a certain
     * relation (highway) is described by a geospatial
     * polygon.
     *
     **************************************************/

    val polygons = registerGeometries(
      buildPolygons(relation_nodes))

    polygons.show
    /**************************************************
     *
     * STAGE #9: Concatenate all (way) polygons that
     * refer to a certain version relation and that
     * share the same `member_role` into a single
     * polygon
     *
     **************************************************/

    val segments = registerSegments(buildSegments(polygons))
    segments

  }
  /**
   * A helper method to concatenate way polygons that
   * refer to a certain highway where possible.
   *
   * The result is a combination of open and closed
   * segments. Note, current implementation focuses
   * on the labeling task of aerial and satellite
   * imagery. Therefore, there is no need to build
   * highways as connected graphs.
   *
   * However, as every segment specifies a start and
   * end explicitly, connected graphs can be built
   * with ease
   */
  private def buildSegments(polygons:DataFrame):DataFrame = {

    val groupCols = List("relation_id", "highway", "tags", "version").map(col)

    val aggCols = List("member_role", "polygon").map(col)
    val colStruct = struct(aggCols: _*)

    val segments = polygons
      .groupBy(groupCols: _*)
      .agg(collect_list(colStruct).as("_collected"))
      .withColumn("segments", explode(UDF.buildSegments(col("_collected"))))
      .withColumn("type",     col("segments").getItem("type"))
      .withColumn("polygons", col("segments").getItem("geometries"))
      .drop("_collected").drop("segments")

    segments

  }
  /**
   * A helper method to aggregate aggregate all nodes
   * that refer to a certain way of a versioned highway
   * into an ordered polygon
   */
  private def buildPolygons(relation_nodes:DataFrame):DataFrame = {

    val aggCols = List("wnode_ix", "wnode_id", "latitude", "longitude").map(col)
    val colStruct = struct(aggCols: _*)

    val groupCols = List("relation_id", "highway", "member_role", "tags", "way_id", "version").map(col)

    val polygons = relation_nodes
      .groupBy(groupCols: _*)
      .agg(collect_list(colStruct).as("_collected"))
      .withColumn("polygon", UDF.buildGeometry(col("_collected")))
      .drop("_collected")
      .sort(col("relation_id").asc)

    polygons

  }

  private def registerGeometries(polygons:DataFrame):DataFrame = {
    val TEMP_GEOMETRY_FILE = s"${rasterZoo}highways.geometry.parquet"
    register(polygons, TEMP_GEOMETRY_FILE)
  }

  private def registerSegments(segments:DataFrame):DataFrame = {
    val TEMP_SEGMENTS_FILE = s"${rasterZoo}highways.segment.parquet"
    register(segments, TEMP_SEGMENTS_FILE)
  }

}
