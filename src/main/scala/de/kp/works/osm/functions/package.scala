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

import de.kp.works.vectorpipe.functions.osm
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

package object functions {
  /**
   * This method determines whether an OSM element
   * (ways or relations) specify an area.
   */
  def is_area:UserDefinedFunction =
    udf((tags: mutable.WrappedArray[Row]) => {
      val mtags = tags.map(tag => {
        val k = new String(tag.getAs[Array[Byte]]("key"))
        val v = new String(tag.getAs[Array[Byte]]("value"))

        (k,v)
      }).toMap

      isArea(mtags)

     })
  /**
   * This method determines whether an OSM element
   * (ways or relations) specify an area.
   */
  def isArea(mtags: Map[String,String]): Boolean = {
    /**
     * This part of the UDF is a copy of the functionality
     * provided by Geotrellis Vectorpipe project
     */
    mtags match {
      case _ if mtags.contains("area") && osm.BooleanValues.toSet
        .intersect(osm.splitDelimitedValues(mtags("area"))).nonEmpty =>

        osm.TruthyValues.toSet.intersect(osm.splitDelimitedValues(mtags("area"))).nonEmpty

      case _ =>
        /* see https://github.com/osmlab/id-area-keys (values are inverted) */
        val matchingKeys = mtags.keySet.intersect(osm.AreaKeys.keySet)

        matchingKeys.exists(k => {
          /* break out semicolon-delimited values */
          val values = osm.splitDelimitedValues(mtags(k))

          /* values that should be considered as lines */
          osm.AreaKeys(k).keySet
            .intersect(values)
            /* at least one key passes the area test */
            .size < values.size
        })
    }
  }

  /**
   * This method evaluates the `tag` column of an OSM-specific
   * dataframe and evaluates whether the provided key matches
   * one of the tag keys. In case of a match, the associated
   * value is returned.
   */
  def key_match(key:String): UserDefinedFunction =
    udf((tags: mutable.WrappedArray[Row]) => {
      val mtags = tags.map(tag => {
        val k = new String(tag.getAs[Array[Byte]]("key"))
        val v = new String(tag.getAs[Array[Byte]]("value"))

        (k,v)
      }).toMap

      keyMatch(key, mtags)
    })

  /**
   * This method evaluates the `tag` column of an OSM-specific
   * dataframe and evaluates whether the provided key matches
   * one of the tag keys. In case of a match, the associated
   * value is returned.
   */
  def keyMatch(key:String, mtags:Map[String,String]): String = {
    if (mtags.contains(key)) mtags(key) else ""
  }

  def query_match(query:Map[String,String]): UserDefinedFunction =
    udf((tags: mutable.WrappedArray[Row]) => {
      val mtags = tags.map(tag => {
        val k = new String(tag.getAs[Array[Byte]]("key"))
        val v = new String(tag.getAs[Array[Byte]]("value"))

        (k,v)
      }).toMap

      queryMatch(query, mtags)
    })

  def queryMatch(query:Map[String,String], mtags:Map[String,String]): Boolean = {
    /*
     * Restrict the tags to those that match
     * the provided query.
     *
     * The current implementation expects that
     * there is at least one match
     */
    val filtered = mtags.filter{case(k,v) =>
      if (!query.contains(k)) false
      else {
        if (query(k).isEmpty) true
        else {
          if (query(k) != v) false else true
        }
      }
    }
    /*
     * If the `tags` contain at least one
     * of the provided query pairs, this
     * method returns `true` otherwise
     * false.
     */
    if (filtered.isEmpty) false else true

  }

}
