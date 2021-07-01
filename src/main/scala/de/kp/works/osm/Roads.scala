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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Roads extends Entities {

  val highway: String = HIGHWAY
  /**
   * This method extracts relations or ways, specified as `highway`,
   * that fall within the provided bounding box.
   */
  def limitHighways(bbox:BBox, mode:String="way"):DataFrame = {

    val ts0 = System.currentTimeMillis

    val ways = limitWays(bbox)

    val ts1 = System.currentTimeMillis
    if (verbose) println(s"Ways restricted to bounding box extracted in ${ts1 - ts0} ms")

    val highways = if (mode == "way") {
      ways
        .withColumn(highway, UDF.keyMatch(highway)(col(TAGS)))
        .filter(not(col(highway) === ""))

    } else {
      /*
       * Load relations and explode to prepare joining
       * with either relations or ways
       */
      val relations = loadRelationMembers

      val ts2 = System.currentTimeMillis
      if (verbose) println(s"Relations exploded into members in ${ts2 - ts1} ms")

      /*
       * The member types commonly used to specify the type
       * of subordinate data is `Way` and `Relation`. It is
       * a reference to the respective `relation` or `way`
       * file.
       *
       * The OSM relation dataset contains members of type
       * `Node` as well, but these relations refer e.g. to
       * `traffic_signals`, `bus_stop` and other point-like
       * data objects.
       */
      val rel_ways = {

        val right = ways.drop("tags", "version")

        /** RELATION -> WAY **/

        val way_members = relations
          .filter(col("member_type") === "Way")
          .join(right, relations("member_id") === right("way_id"), "inner")
          .drop("member_id", "member_type")
          /*
           * Restrict to those tagged with `highway`
           */
          .withColumn(highway, UDF.keyMatch(highway)(col(TAGS)))
          .filter(not(col(highway) === ""))

        val ts3 = System.currentTimeMillis
        if (verbose) println(s"Relations connected to ways in ${ts3 - ts2} ms")

        /** RELATION -> RELATION -> WAY */

        val rel_members = joinRelsWithRels(relations)

        val ts4= System.currentTimeMillis
        if (verbose) println(s"Relations connected to relations in ${ts4 - ts3} ms")

        /*
         * The annotated relations now reference `Node`, `Relation`
         * and `Way` members. Here, we restrict to `Way` members that
         * fill within the provided bounding box.
         *
         * The result is specified by the following columns:
         *
         * relation_id, tags, version, member_role, way_id, geometry, highway
         *
         * Note: A single relation can be described by multiple ways
         * that can be distinguished by the associated member_role, e.g.
         * inner, outer etc.
         */
        val way_rel_members = rel_members
          .filter(col("member_type") === "Way")
          .join(right, rel_members("member_id") === right("way_id"), "inner")
          .drop("member_id", "member_type")
          /*
           * Restrict to those tagged with `highway`
           */
          .withColumn(highway, UDF.keyMatch(highway)(col(TAGS)))
          .filter(not(col(highway) === ""))

        val ts5 = System.currentTimeMillis
        if (verbose) println(s"Connected relations connected to ways in ${ts5 - ts4} ms")

        /**
         * Unify relations that are directly built from ways
         * and those that reference relations that finally
         * reference ways.
         */
        way_members.union(way_rel_members)

      }
      /*
       * Relations specified by ways are different from way,
       * i.e. a relation can be contain multiple geometries,
       * while each way is defined by a single geometry.
       */
      rel_ways

    }

    highways

  }
  /**
   * This method transforms OSM relations of type
   * (key) `highway` into connected geospatial
   * geometries.
   *
   * These geometries are distinguished by `relation_id`,
   * highway type, `tags` and `version`.
   */
  def relations2Highways:DataFrame = {

    var relations = loadRelations.drop(DROP_COLS: _*)

    /**************************************************
     *
     * STAGE #1: Restrict the available relations
     * to those that match the predefined tag key
     *
     **************************************************/

    val highways = relations
      .withColumn(highway, UDF.keyMatch(highway)(col(TAGS)))
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
    /*
     * The member types commonly used to specify the type
     * of subordinate data is `Way` and `Relation`. It is
     * a reference to the respective `relation` or `way`
     * file.
     *
     * The OSM relation dataset contains members of type
     * `Node` as well, but these relations refer e.g. to
     * `traffic_signals`, `bus_stop` and other point-like
     * data objects.
     */

    val dropCols = List("timestamp", "changeset", "uid", "user_sid", "tags", "version")

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

    val way_members = joinRelsWithWays(members)

    /**************************************************
     *
     * STAGE #4: Join members with relations to retrieve
     * the relation elements that describe a highway
     *
     **************************************************/

    val relation_members = joinRelsWithRels(members)

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

    val way_relation_members = joinRelsWithWays(relation_members)

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

    val nodes = loadNodes.drop(dropCols: _*)

    val relation_nodes = nodeset
      .join(nodes, way_members("node_id") === nodes("id"), "inner")
      .drop("id")

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
      .withColumn("polygons", col("segments").getItem("polygons"))
      .drop("_collected").drop("segments")

    segments

  }
  /**
   * A helper method to aggregate aggregate all nodes
   * that refer to a certain way of a versioned highway
   * into an ordered polygon
   */
  private def buildPolygons(relation_nodes:DataFrame):DataFrame = {

    val aggCols = List("node_ix", "node_id", "latitude", "longitude").map(col)
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
