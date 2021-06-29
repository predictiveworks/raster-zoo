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
  /**
   * This method restricts the available nodes
   * to those that fall in the provided bounding
   * box.
   *
   * The remaining dataset is specified by the
   * node identifier and the associated coordinates.
   */
  def limitNodes(bbox:BBox):DataFrame = {

    val dropCols = List("timestamp", "changeset", "uid", "user_sid", "tags", "version")
    val nodes = loadNodes.drop(dropCols: _*)

    nodes.filter(UDF.limit2bbox(bbox)(col("latitude"), col("longitude")))

  }
  /**
   * This method restricts the available ways to
   * those that contain nodes that fall within
   * the provided bounding box.
   *
   * In addition, these limited ways are annotated
   * by their geometry, i.e. a linestring or polygon.
   */
  def limitWays(bbox:BBox):DataFrame = {
    /**
     * STEP #1: Compute all nodes that fall
     * within the provided bounding box
     */
    val nodes = limitNodes(bbox)
    /**
     * STEP #2: Load ways and prepare for join
     * with computed nodes
     */
    val dropCols = List("timestamp", "changeset", "uid", "user_sid")
    val ways = loadWays.drop(dropCols: _*)
      .withColumnRenamed("id", "way_id")
      /*
       * Explode the nodes that describe each way to prepare
       * subsequent assignment of geo coordinates
       */
      .withColumn("node", explode(UDF.extractNodes(col("nodes"))))
      .withColumn("node_ix", col("node").getItem("nix"))
      .withColumn("node_id", col("node").getItem("nid"))
      .drop("nodes", "node")
    /**
     * STEP #3: Join ways with computed nodes. This step prepares
     * the computation of the respective linestring or polygons
     * that describe every way
     */
    val annotated = ways
      .join(nodes, ways("node_id") === nodes("id"), "inner").drop("id")

    /**
     * STEP #4: Assign geometry to every way and thereby
     * collect all geospatial points in specified order
     */
    val groupCols = List("way_id", "tags", "version").map(col)

    val aggCols = List("node_id", "node_ix", "latitude", "longitude").map(col)
    val colStruct = struct(aggCols: _*)

    val collected = annotated
      .groupBy(groupCols: _*)
      .agg(collect_list(colStruct).as("_collected"))
      .withColumn("geometry", UDF.buildGeometry(col("_collected")))
      .drop("_collected")

    collected

  }

  /**
   * This method extracts relations or ways, specified as `highway`,
   * that fall within the provided bounding box.
   */
  def limitHighways(bbox:BBox, mode:String="way"):DataFrame = {

    val highway = HIGHWAY
    val ways = limitWays(bbox)

    val highways = if (mode == "way") {
      ways
        .withColumn(highway, UDF.keyMatch(highway)(col(TAGS)))
        .filter(not(col(highway) === ""))

    } else {
      throw new Exception(s"Not implemented yet")
    }

    highways

  }
  /**
   * This method transforms OSM relations of type
   * (key) `highway` into connected geospatial
   * polygons.
   *
   * These polygons are distinguished by `relation_id`,
   * highway type, `tags` and `version`.
   */
  def buildHighways:DataFrame = {

    val highway = HIGHWAY

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
    val members = highways
      .withColumn("member", explode(UDF.extractMembers(col("members"))))
      .withColumn("member_id",   col("member").getItem("mid"))
      .withColumn("member_role", col("member").getItem("mrole"))
      .withColumn("member_type", col("member").getItem("mtype"))
      .withColumnRenamed("id", "relation_id")
      .drop("members").drop("member")

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

    val ways = loadWays.drop(dropCols: _*)

    val way_members = members
      .filter(col("member_type") === "Way")
      .join(ways, members("member_id") === ways("id"), "inner")
      /*
       * The `way_id` aggregates all nodes that refer to the
       * same. It is important to keep this parameter as it
       * enables to define each way as a polygon.
       */
      .withColumnRenamed("id", "way_id")
      /*
       * Explode the nodes that describe each way to prepare
       * subsequent assignment of geo coordinates
       */
      .withColumn("node", explode(UDF.extractNodes(col("nodes"))))
      .withColumn("node_ix", col("node").getItem("nix"))
      .withColumn("node_id", col("node").getItem("nid"))
      /*
       * Drop processing specific columns and those that
       * contain redundant information; note, `member_role`
       * must not be skipped as this information is used to
       * when building polygons.
       */
      .drop("nodes").drop("node").drop("id")
      .drop("member_id", "member_type")

    /**************************************************
     *
     * STAGE #4: Join members with relations to retrieve
     * the relation elements that describe a highway
     *
     **************************************************/

    relations = relations.drop("tags", "version")

    val relation_members = members
      .filter(col("member_type") === "Relation")
      .join(relations.drop("version"), members("member_id") === relations("id"), "inner")
      /*
       * Drop previous member columns before the respective
       * way members can be exploded; also remove the relation
       * id to avoid conflicts with subsequent way join.
       */
      .drop("id", "member_id").drop("member_role").drop("member_type")
      .withColumn("member", explode(UDF.extractMembers(col("members"))))
      .withColumn("member_id",   col("member").getItem("mid"))
      .withColumn("member_role", col("member").getItem("mrole"))
      .withColumn("member_type", col("member").getItem("mtype"))
      .drop("members").drop("member")

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

    val way_relation_members = relation_members
      /*
       * The current implementation supports no
       * multi-level nested definitions of relations,
       * e.g. relations that are defined by relation
       * members that are defined by relation members
       * etc.
       */
      .filter(col("member_type") === "Way")
      .join(ways, relation_members("member_id") === ways("id"), "inner")
      /*
       * The `way_id` aggregates all nodes that refer to the
       * same. It is important to keep this parameter as it
       * enables to define each way as a polygon.
       */
      .withColumnRenamed("id", "way_id")
      /*
       * Explode the nodes that describe each way to prepare
       * subsequent assignment of geo coordinates
       */
      .withColumn("node", explode(UDF.extractNodes(col("nodes"))))
      .withColumn("node_ix", col("node").getItem("nix"))
      .withColumn("node_id", col("node").getItem("nid"))
      /*
       * Drop processing specific columns and those that
       * contain redundant information; note, `member_role`
       * must not be skipped as this information is used to
       * when building polygons.
       */
      .drop("nodes").drop("node")
      .drop("member_type").drop("member_id")

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

    val polygons = registerPolygons(
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

  private def registerPolygons(polygons:DataFrame):DataFrame = {
    val TEMP_POLYGON_FILE = s"${rasterZoo}highways.polygon.parquet"
    register(polygons, TEMP_POLYGON_FILE)
  }

  private def registerSegments(segments:DataFrame):DataFrame = {
    val TEMP_SEGMENTS_FILE = s"${rasterZoo}highways.segment.parquet"
    register(segments, TEMP_SEGMENTS_FILE)
  }

  private def register(dataframe:DataFrame, path:String):DataFrame = {
    dataframe.write.mode(SaveMode.Overwrite).parquet(path)
    session.read.parquet(path)
  }

}
