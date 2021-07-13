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

import de.kp.works.spark.Geometry

import scala.collection.mutable

object GeometryUtils extends Serializable {

  private val verbose = true

  private def flattenGeometry(polygons:Seq[Geometry]):Geometry = {

    val uuid  = java.util.UUID.randomUUID.toString

    val snode = polygons.head.snode
    val enode = polygons.last.enode

    val geometry = if (snode == enode) "polygon" else "linestring"
    val path = polygons.flatMap(polygon => polygon.path)

    Geometry(uuid, snode, enode, path, path.length, geometry)

  }

  def buildSegments(ways:Seq[Geometry]):Seq[Geometry] = {

    val segments = mutable.ArrayBuffer.empty[Geometry]
    /**
     * STEP #1: Closed ways are excluded from the
     * respective processing. We expect that these
     * ways are finished already.
     */
    val closed = ways.filter(way => way.snode == way.enode)
    closed.foreach(way => segments += way)
    /**
     * STEP #2: The remaining part of this method
     * focuses on open ways and tries to concat
     * them.
     */
    val open = ways.filter(way => way.snode != way.enode)
    if (open.isEmpty) {
      /**
       * The provided ways specify a sequence of
       * closed ways. In this case, nothing is
       * left to do.
       */
      return segments

    }

    var orderedSeq:Seq[Geometry] = Seq(open.head)
    var unorderedSeq = open.tail
    /**
     * This is an algorithm to concat the available
     * in the right order; this algorithm appends
     * a certain way either at the beginning or the
     * end of the order sequence
     */
    while (unorderedSeq.nonEmpty) {
      /*
       * The starting node of the ordered sequence
       */
      var snode = orderedSeq.head.snode
      /*
       * The ending node of the ordered sequence
       */
      var enode = orderedSeq.last.enode
      /*
       * Tests showed that the ways provided by OSM
       * can contain loops or rings.
       *
       * This implementation removes these rings from
       * the dataset
       */
      if (snode == enode) {

        if (verbose)
          println(s"[INFO] Ring of size ${orderedSeq.size} detected")

        segments += flattenGeometry(orderedSeq)
        /*
         * At this stage, we expect that the unordered
         * sequence of way is not empty and skip the
         * ordered sequence and "re-start" the algorithm
         */
        orderedSeq = Seq(unorderedSeq.head)
        unorderedSeq = unorderedSeq.tail

        snode = orderedSeq.head.snode
        enode = orderedSeq.last.enode

      }

      /*
       * Move through all remaining unordered ways
       * and determine those that either match the
       * starting or ending node
       */
      var found = false
      val iterator = unorderedSeq.iterator

      while (iterator.hasNext && !found) {

        val way = iterator.next()
        if (snode == way.snode) {
          /*
           * In this case, the respective order of the
           * way points must be reverted, and the way
           * is added as first element of the ordered
           * sequence
           */
          val geometry = if (way.enode == way.snode) "polygon" else "linestring"
          val newPolygon =
            Geometry(
              uuid=way.uuid, snode=way.enode, enode=way.snode,
              path=way.path.reverse, length=way.path.length, geometry=geometry)

          orderedSeq = Seq(newPolygon) ++ orderedSeq
          /*
           * Remove the detected way from the list of
           * unordered ways
           */
          unorderedSeq = unorderedSeq.filter(w => w.uuid != way.uuid)
          found = true
        }

        else if (enode == way.enode) {
          /*
           * In this case, the respective order of the
           * way points must be reverted, and the way
           * is added as last element of the ordered
           * sequence
           */
          val geometry = if (way.enode == way.snode) "polygon" else "linestring"
          val newPolygon =
            Geometry(
              uuid=way.uuid, snode=way.enode, enode=way.snode,
              path=way.path.reverse, length=way.path.length, geometry)

          orderedSeq = orderedSeq ++ Seq(newPolygon)
          /*
           * Remove the detected way from the list of
           * unordered ways
           */
          unorderedSeq = unorderedSeq.filter(w => w.uuid != way.uuid)
          found = true
        }

        else if (snode == way.enode) {
          /*
           * This is a regular situation and the way
           * is added as first element of the ordered
           * sequence
           */
          orderedSeq = Seq(way) ++ orderedSeq
          /*
           * Remove the detected way from the list of
           * unordered ways
           */
          unorderedSeq = unorderedSeq.filter(w => w.uuid != way.uuid)
          found = true
        }

        else if (enode == way.snode) {
          /*
           * This is a regular situation and the way
           * is added as last element of the ordered
           * sequence
           */
          orderedSeq = orderedSeq ++ Seq(way)
          /*
           * Remove the detected way from the list of
           * unordered ways
           */
          unorderedSeq = unorderedSeq.filter(w => w.uuid != way.uuid)
          found = true
        }

        else {/* Do nothing */}

      }

      if (!found) {

        segments += flattenGeometry(orderedSeq)
        /*
         * Check whether the unordered sequences
         * of ways is still not empty
         */
        if (unorderedSeq.nonEmpty) {

          orderedSeq = Seq(unorderedSeq.head)
          unorderedSeq = unorderedSeq.tail

        }

      }
    }

    if (segments.isEmpty)
      segments += flattenGeometry(orderedSeq)

    segments

  }

}
