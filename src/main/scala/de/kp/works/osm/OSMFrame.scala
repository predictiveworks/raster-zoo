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
import de.kp.works.spark.{Session, UDF}
import org.apache.spark.sql.functions.{col, collect_list, explode, not, struct}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
/*
 * The [Member] case class determines each member that
 * specifies a certain relation.
 */
case class Member(mid:Long, mrole:String, mtype:String)
/*
 * The [Node] case class determines each node that
 * specifies a certain way with a specific relation.
 */
case class Node(nix:Int, nid:Long)

case class Query(key:String, params:Map[String,String])

trait OSMFrame extends OSMParams {

  protected final val TAGS:String = "tags"

  /**
   * Indicator to determine whether data operations
   * print intermediate messages or not
   */
  protected val verbose = true
  /**
   * The list columns that are excluded when preparing
   * relations, ways and nodes
   */
  protected final val DROP_COLS:Seq[String] =
    List("timestamp", "changeset", "uid", "user_sid")

  protected val session: SparkSession = Session.getSession

  /**
   * GEOSPATIAL DATA OPERATIONS
   */

  /**
   * This method restricts the available nodes
   * to those that fall in the provided bounding
   * box.
   *
   * The remaining dataset is specified by the
   * node identifier and the associated coordinates.
   *
   * It is intended to be used in join operations,
   * e.g. with ways
   */
  protected def limitNodes(bbox:BBox, nodePath:String):DataFrame = {

    val dropCols = DROP_COLS ++ List("tags", "version")
    val nodes = loadNodes(nodePath).drop(dropCols: _*)
    /*
     * Output columns: node_id, latitude, longitude
     */
    nodes.filter(UDF.limit2bbox(bbox)(col("latitude"), col("longitude")))

  }
  /**
   * This method restricts the available nodes
   * to those that fall in the provided bounding
   * box.
   *
   * In addition to this geospatial filtering,
   * those nodes are returned that match the query
   * and the provided key.
   *
   * If the `key` is available, the respective value
   * is extracted into its own column
   */
  protected def limitNodes(bbox:BBox, query:Query, nodePath:String):DataFrame = {

    val nodes = loadNodes(nodePath).drop(DROP_COLS: _*)
    /**
     * STEP #1: Restrict nodes to the provided bounding box
     */
    var limitedNodes = nodes
      .filter(UDF.limit2bbox(bbox)(col("latitude"), col("longitude")))

    /**
     * STEP #2: Restrict nodes to those that match the
     * provided query
     */
    if (query.params.nonEmpty)
      limitedNodes = limitedNodes.filter(UDF.queryMatch(query.params)(col(TAGS)))

    /**
     * STEP #3: Extract the provided `key` value
     */
    if (query.key.nonEmpty)
      limitedNodes = limitedNodes
        .withColumn(query.key, UDF.keyMatch(query.key)(col(TAGS)))
        .filter(not(col(query.key) === ""))

    limitedNodes
      .drop("tags", "version")

  }
  /**
   * This method restricts the available ways to
   * those that contain nodes that fall within
   * the provided bounding box.
   *
   * In addition, these limited ways are annotated
   * with their geometry, i.e. a linestring or polygon.
   */
  protected def limitWays(bbox:BBox, nodePath:String, wayPath:String):DataFrame = {
    /**
     * STEP #1: Compute all nodes that fall
     * within the provided bounding box
     */
    val limitedNodes = limitNodes(bbox, nodePath)
    /**
     * STEP #2: Load ways and join with limited nodes.
     * This step prepares the computation of the respective
     * linestring or polygons that describe every way
     */
    val wayNodes = buildWayNodes(
      loadWays(wayPath).drop(DROP_COLS: _*), limitedNodes)
    /**
     * STEP #3: Assign geometry to every way and thereby
     * collect all geospatial points in specified order
     */
    val columns = List.empty[String]
    annotateWayGeometry(wayNodes, columns)

  }
  /**
   * This method restricts the available ways to
   * those that contain nodes that fall within
   * the provided bounding box.
   *
   * In addition to this geospatial filtering,
   * those ways are returned that match the query
   * and the provided key.
   *
   * If the `key` is available, the respective value
   * is extracted into its own column
   *
   * Finally, these limited ways are annotated with
   * their geometry, i.e. a linestring or polygon.
   */
  protected def limitWays(bbox:BBox, query:Query, nodePath:String, wayPath:String):DataFrame = {
    /**
     * STEP #1: Compute all nodes that fall
     * within the provided bounding box
     */
    val limitedNodes = limitNodes(bbox, nodePath)
    /**
     * STEP #2: Load ways and limit to those
     * that match the provided key
     */
    var limitedWays = loadWays(wayPath).drop(DROP_COLS: _*)
    /**
     * STEP #3: Restrict nodes to those that match the
     * provided query
     */
    if (query.params.nonEmpty)
      limitedWays = limitedWays.filter(UDF.queryMatch(query.params)(col(TAGS)))
    /**
     * STEP #4: Limit ways to those that match the
     * provided key
     */
    if (query.key.nonEmpty) {
      limitedWays = limitedWays
        .withColumn(query.key, UDF.keyMatch(query.key)(col(TAGS)))
        .filter(not(col(query.key) === ""))
    }

    /**
     * STEP #5: Join limited ways with limited nodes.
     * This step prepares the computation of the respective
     * linestring or polygons that describe every way
     */
    val wayNodes = buildWayNodes(limitedWays, limitedNodes)
    /**
     * STEP #6: Assign geometry to every way and thereby
     * collect all geospatial points in specified order
     */
    val columns = if (query.key.nonEmpty) List(query.key) else List.empty[String]
    annotateWayGeometry(wayNodes, columns)

  }

  /**
   * Method to annotate OSM ways with specified geometry
   * with the respective bounding box of the linestring
   * or polygon
   */
  protected def annotateWayBBox(wayNodes:DataFrame):DataFrame = {
    wayNodes
      .withColumn("bbox", UDF.geometry2BBox(col(geometryColName)))
  }
  /**
   * Method to assign geometry to every way and thereby
   * collect all geospatial points in specified order.
   *
   * The method expects way-nodes.
   */
  protected def annotateWayGeometry(wayNodes:DataFrame, columns:List[String], mode:String="jts"):DataFrame = {
    /*
     * Annotate `wayNodes` with the `isArea` flag
     * to indicate whether an `area` is specified
     * or not.
     */
    val annotated = wayNodes
      .withColumn("is_area", UDF.isArea(col("tags")))

    /*
     * Aggregate all nodes that refer to a specific way.
     * This requires a grouping & aggregation task
     */
    val aggregated = {

      val baseCols = List("way_id", "tags", "is_area", "version").map(col)
      val groupCols = baseCols ++ columns.map(col)

      val aggCols = List("wnode_id", "wnode_ix", "latitude", "longitude").map(col)
      val colStruct = struct(aggCols: _*)

      annotated
        .groupBy(groupCols: _*)
        .agg(collect_list(colStruct).as("_collected"))

    }
    /*
     * Determine how to build the respective geometry
     * from the aggregated nodes
     */
    if (mode == "jts") {
      aggregated
        .withColumn(geometryColName, UDF.buildJtsGeometry(col("_collected"), col("is_area")))
        .drop("_collected")

    } else {
      aggregated
        .withColumn(geometryColName, UDF.buildGeometry(col("_collected")))
        .drop("_collected")
    }

  }
  /**
   * BASE DATA (PREPARATION) OPERATIONS
   */
  protected def buildWayNodes(ways:DataFrame, nodes:DataFrame):DataFrame = {
    /**
     * STEP #1: Check whether the columns of the provided
     * `nodes` do intersect with `ways` columns and remove
     * identified columns
     */
    val nodesFields = nodes.schema.fieldNames

    val dropCols = (DROP_COLS ++ List("tags", "version"))
      .filter(colname => nodesFields.contains(colname))

    val right = if (dropCols.isEmpty) nodes else nodes.drop(dropCols: _*)
    /**
     * STEP #2: Check whether columns of the provided
     * `ways` indicate that this dataframe is exploded
     * to its nodes
     */
    val waysFields = ways.schema.fieldNames
    val left = if (waysFields.contains("wnode_id")) {
      ways

    } else {
      ways
        /*
         * Explode the nodes that describe each way to prepare
         * subsequent assignment of geo coordinates
         */
        .withColumn("node", explode(UDF.extractNodes(col("nodes"))))
        .withColumn("wnode_ix", col("node").getItem("nix"))
        .withColumn("wnode_id", col("node").getItem("nid"))
        .drop("nodes", "node")
    }
    /**
     * STEP #3: Joint left and right dataframe with an
     * `inner` join.
     */
    val wayNodes = left
      .join(right, left("wnode_id") === right("node_id"), "inner").drop("node_id")

    wayNodes

  }

  protected def buildRelationRelations(relations1:DataFrame, relations2:DataFrame):DataFrame = {
    /**
     * STEP #1: Check whether the columns of the provided
     * `relations2` do intersect with `relations1` columns
     * and remove identified columns
     */
    val relations2Fields = relations2.schema.fieldNames
    /*
     * The current implementation is restricted to `relations2`
     * that are not exploded to its nodes
     */
    if (relations2Fields.contains("member_type"))
      throw new Exception(s"The `right` relation must not be exploded to its members.")

    val dropCols = (DROP_COLS ++ List("tags", "version"))
      .filter(colname => relations2Fields.contains(colname))

    val right = (if (dropCols.isEmpty) relations2 else relations2.drop(dropCols: _*))
      .withColumnRenamed("relation_id", "id")
    /**
     * STEP #2: Check whether columns of the provided
     * `relations1` indicate that this dataframe is
     * exploded to its members
     */
    val relations1Fields = relations1.schema.fieldNames
    val left = if (relations1Fields.contains("member_type")) {
      relations1

    } else {
      buildRelationMembers(relations1)
    }
    /**
     * STEP #3: Joint left and right dataframe with an
     * `inner` join and thereby restrict to `Relation`
     * members.
     */
    val relationRelations = left
      .join(right, left("member_id") === right("id"), "inner")

    /**
     * STEP #4: Explode the (right) members of the
     * `relationRelations` and prepare for subsequent
     * join with nodes, relations or ways.
     */
    relationRelations
      /*
       * Drop previous member columns before the respective
       * relation members can be exploded
       */
      .drop("member_id").drop("member_role", "member_type")
      /*
       * Explode members of the subordinate `relations1`.
       * The resulting dataset contains members that are
       * expected to reference ways (or points).
       *
       * The current implementation does not support nested
       * relation --> relation --> relation.
       */
      .withColumn("member", explode(UDF.extractMembers(col("members"))))
      .withColumn("member_id",   col("member").getItem("mid"))
      .withColumn("member_role", col("member").getItem("mrole"))
      .withColumn("member_type", col("member").getItem("mtype"))
      .drop("members", "member").drop("id")

  }

  protected def buildRelationWays(relations:DataFrame, ways:DataFrame):DataFrame = {
    /**
     * STEP #1: Check whether the columns of the
     * provided `ways` do intersect with `relations`
     * columns and remove identified columns
     */
    val waysFields = ways.schema.fieldNames
    val dropCols = (DROP_COLS ++ List("tags", "version"))
      .filter(colname => waysFields.contains(colname))

    val right = if (dropCols.isEmpty) ways else ways.drop(dropCols: _*)
    /**
     * STEP #2: Check whether columns of the provided
     * `relations` indicate that this dataframe is
     * exploded to its members
     */
    val relationsFields = relations.schema.fieldNames
    val left = if (relationsFields.contains("member_type")) {
      relations

    } else {
      buildRelationMembers(relations)
    }
    /**
     * STEP #3: Joint left and right dataframe with an
     * `inner` join and thereby restrict to `Way` members.
     */
    val relationWays = left
      .filter(col("member_type") === "Way")
      .join(right, left("member_id") === right("way_id"), "inner")
    /**
     * STEP #4: Explode the nodes of the `relationWays`
     * and prepare for a final join with a nodes dataframe
     * later on
     */
    relationWays
      .withColumn("node", explode(UDF.extractNodes(col("nodes"))))
      .withColumn("wnode_ix", col("node").getItem("nix"))
      .withColumn("wnode_id", col("node").getItem("nid"))
      /*
       * Drop processing specific columns and those that
       * contain redundant information; note, `member_role`
       * must not be skipped as this information is used to
       * when building polygons.
       */
      .drop("nodes", "node")
      .drop("member_id", "member_type")

  }
  /**
   * This is a helper method to load OSM relations and
   * extract and explode the respective members
   */
  protected def loadRelationMembers(relationPath:String):DataFrame = {
    /*
      * Load relations and explode to prepare joining
      * with either relations or ways
      */
    val relations = loadRelations(relationPath).drop(DROP_COLS: _*)
    buildRelationMembers(relations)

  }

  /**
   * This is a helper method to extract and explode
   * relation members; members can be nodes, relations
   * and ways.
   *
   * The column `member_type` determine which members
   * are referenced.
   */
  protected def buildRelationMembers(relations:DataFrame):DataFrame = {

    relations
      .withColumn("member", explode(UDF.extractMembers(col("members"))))
      .withColumn("member_id",   col("member").getItem("mid"))
      .withColumn("member_role", col("member").getItem("mrole"))
      .withColumn("member_type", col("member").getItem("mtype"))
      .drop("members").drop("member")

  }

  /**
   * BASE PARQUET FILE OPERATIONS
   */
  protected def loadNodes(nodePath:String):DataFrame = {
    if (nodePath.isEmpty)
      throw new Exception("The path to the `nodes` parquet file is not provided.")

    session.read.parquet(nodePath)
      /*
       * Nodes take part on a variety of data
       * operations; therefore, the `id` column
       * is renamed as `node_id`
       */
      .withColumnRenamed("id", "node_id")
  }

  protected def loadRelations(relationPath:String):DataFrame = {
    if (relationPath.isEmpty)
      throw new Exception("The path to the `relations` parquet file is not provided.")

    session.read.parquet(relationPath)
      /*
       * Relations take part on a variety of data
       * operations; therefore, the `id` column
       * is renamed as `relation_id`
       */
      .withColumnRenamed("id", "relation_id")
  }

  protected def loadWays(wayPath:String):DataFrame = {
    if (wayPath.isEmpty)
      throw new Exception("The path to the `ways` parquet file is not provided.")

    session.read.parquet(wayPath)
      /*
       * Ways take part on a variety of data
       * operations; therefore, the `id` column
       * is renamed as `way_id`
       */
      .withColumnRenamed("id", "way_id")

  }

  protected def register(dataframe:DataFrame, path:String):DataFrame = {
    dataframe.write.mode(SaveMode.Overwrite).parquet(path)
    session.read.parquet(path)
  }

}
