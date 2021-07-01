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
import de.kp.works.spark.Session
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, explode, struct}
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

abstract class Entities {

  protected val verbose = true

  protected final val HIGHWAY:String = "highway"
  protected final val TAGS:String = "tags"
  /*
   * The list columns that are not taken into account
   */
  protected final val DROP_COLS:Seq[String] =
    List("timestamp", "changeset", "uid", "user_sid")

  protected val session: SparkSession = Session.getSession
  /*
   * The path to the OSM nodes parquet file.
   */
  protected var nodePath:String = ""
  /*
   * The path to the OSM relations parquet file.
   */
  protected var relationPath:String = ""
  /*
   * The path to the OSM ways parquet file.
   */
  protected var wayPath:String = ""
  /*
   * The path to the FS parquet folder that contains
   * intermediate and final computation results
   */
  protected var rasterZoo:String = ""

  def setNodePath(value:String): Entities = {
    nodePath = value
    this
  }

  def setRelationPath(value:String): Entities = {
    relationPath = value
    this
  }

  def setWayPath(value:String): Entities = {
    wayPath = value
    this
  }

  def setRasterZoo(value:String): Entities = {
    rasterZoo = value
    this
  }

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
  protected def limitNodes(bbox:BBox):DataFrame = {

    val dropCols = DROP_COLS ++ List("tags", "version")
    val nodes = loadNodes.drop(dropCols: _*)
    /*
     * id, latitude, longitude
     */
    nodes.filter(UDF.limit2bbox(bbox)(col("latitude"), col("longitude")))

  }

  protected def loadNodes:DataFrame = {
    if (nodePath.isEmpty)
      throw new Exception("The path to the `nodes` parquet file is not provided.")

    session.read.parquet(nodePath)

  }

  protected def loadRelations:DataFrame = {
    if (relationPath.isEmpty)
      throw new Exception("The path to the `relations` parquet file is not provided.")

    session.read.parquet(relationPath)

  }
  /**
   * This is a helper method to extract and explode
   * relation members
   */
  protected def buildRelationMembers(relations:DataFrame):DataFrame = {

    relations
      .withColumnRenamed("id", "relation_id")
      .withColumn("member", explode(UDF.extractMembers(col("members"))))
      .withColumn("member_id",   col("member").getItem("mid"))
      .withColumn("member_role", col("member").getItem("mrole"))
      .withColumn("member_type", col("member").getItem("mtype"))
      .drop("members").drop("member")

  }
  /**
   * This is a helper method to load OSM relations and
   * extract and explode the respective members
   */
  protected def loadRelationMembers:DataFrame = {
    /*
      * Load relations and explode to prepare joining
      * with either relations or ways
      */
    val relations = loadRelations.drop(DROP_COLS: _*)
      .withColumnRenamed("id", "relation_id")
      .withColumn("member", explode(UDF.extractMembers(col("members"))))
      .withColumn("member_id",   col("member").getItem("mid"))
      .withColumn("member_role", col("member").getItem("mrole"))
      .withColumn("member_type", col("member").getItem("mtype"))
      .drop("members", "member")

    relations

  }

  /**
   * This method restricts the available ways to
   * those that contain nodes that fall within
   * the provided bounding box.
   *
   * In addition, these limited ways are annotated
   * by their geometry, i.e. a linestring or polygon.
   */
  protected def limitWays(bbox:BBox):DataFrame = {
    /**
     * STEP #1: Compute all nodes that fall
     * within the provided bounding box
     */
    val nodes = limitNodes(bbox)
    /**
     * STEP #2: Load ways and prepare for join
     * with computed nodes
     */
    val ways = loadWays.drop(DROP_COLS: _*)
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

  protected def loadWays:DataFrame = {
    if (wayPath.isEmpty)
      throw new Exception("The path to the `ways` parquet file is not provided.")

    session.read.parquet(wayPath)

  }

  protected def joinRelsWithRels(relation1:DataFrame):DataFrame = {

    val dropCols = DROP_COLS ++ List("tags", "version")
    val relations2 = loadRelations.drop(dropCols: _*)
    /*
     * This method expects that the `relations` dataframe
     * is exploded to its members
     */
    relation1
      /*
       * Restrict the leading dataframe to
       * to those with relation members
       */
      .filter(col("member_type") === "Relation")
      .join(relations2, relation1("member_id") === relations2("id"), "inner")
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

  protected def joinRelsWithWays(relations:DataFrame):DataFrame = {

    val dropCols = DROP_COLS ++ List("tags", "version")
    val ways = loadWays.drop(dropCols: _*)
    /*
     * This method expects that the `relations` dataframe
     * is exploded to its members
     */
    relations
      .filter(col("member_type") === "Way")
      .join(ways, relations("member_id") === ways("id"), "inner")
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
      .drop("nodes", "node")
      .drop("member_id", "member_type")

   }

  protected def register(dataframe:DataFrame, path:String):DataFrame = {
    dataframe.write.mode(SaveMode.Overwrite).parquet(path)
    session.read.parquet(path)
  }

}
