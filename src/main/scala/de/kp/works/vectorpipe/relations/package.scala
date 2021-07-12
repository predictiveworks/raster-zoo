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

import org.locationtech.jts.geom._
import de.kp.works.vectorpipe.relations.utils.{
  PartialCoordinateSequence,
  ReversedCoordinateSequence,
  VirtualCoordinateSequence,
  isEqual
}

import scala.annotation.tailrec
import scala.collection.GenTraversable

package object relations {

  // join segments together
  @tailrec
  def connectSegments(segments: GenTraversable[VirtualCoordinateSequence],
                      lines: Seq[CoordinateSequence] = Vector.empty[CoordinateSequence])
    : GenTraversable[CoordinateSequence] = {
    segments match {
      case Nil =>
        lines
      case Seq(h, t @ _*) =>
        val x = h.getX(h.size - 1)
        val y = h.getY(h.size - 1)

        t.find(line => x == line.getX(0) && y == line.getY(0)) match {
          case Some(next) =>
            connectSegments(h.append(new PartialCoordinateSequence(next, 1)) +: t.filterNot(line =>
                              isEqual(line, next)),
                            lines)
          case None =>
            t.find(line => x == line.getX(line.size - 1) && y == line.getY(line.size - 1)) match {
              case Some(next) =>
                connectSegments(h.append(
                                  new PartialCoordinateSequence(
                                    new ReversedCoordinateSequence(next),
                                    1)) +: t.filterNot(line => isEqual(line, next)),
                                lines)
              case None => connectSegments(t, lines :+ h)
            }
        }
    }
  }

  def connectSegments(segments: GenTraversable[Geometry])(
      implicit geometryFactory: GeometryFactory): GenTraversable[LineString] =
    connectSegments(
      segments
        .flatMap {
          case geom: LineString => Some(geom.getCoordinateSequence)
          case _                => None
        }
        .map(s => new VirtualCoordinateSequence(Seq(s)))
    ).map(geometryFactory.createLineString)

  // since GeoTrellis's GeometryFactory is unavailable
  implicit val geometryFactory: GeometryFactory = new GeometryFactory()

  // join segments together into rings
  @tailrec
  def formRings(segments: GenTraversable[VirtualCoordinateSequence],
                        rings: Seq[CoordinateSequence] = Vector.empty[CoordinateSequence])
    : GenTraversable[CoordinateSequence] = {
    segments match {
      case Nil =>
        rings
      case Seq(h, t @ _*) if h.getX(0) == h.getX(h.size - 1) && h.getY(0) == h.getY(h.size - 1) =>
        formRings(t, rings :+ h)
      case Seq(h, t @ _*) =>
        val x = h.getX(h.size - 1)
        val y = h.getY(h.size - 1)

        formRings(
          t.find(line => x == line.getX(0) && y == line.getY(0)) match {
            case Some(next) =>
              h.append(new PartialCoordinateSequence(next, 1)) +: t.filterNot(line =>
                isEqual(line, next))
            case None =>
              t.find(line => x == line.getX(line.size - 1) && y == line.getY(line.size - 1)) match {
                case Some(next) =>
                  h.append(new PartialCoordinateSequence(new ReversedCoordinateSequence(next), 1)) +: t
                    .filterNot(line => isEqual(line, next))
                case None => throw new AssemblyException("Unable to connect segments.")
              }
          },
          rings
        )
    }
  }

  def formRings(segments: GenTraversable[LineString])(
      implicit geometryFactory: GeometryFactory): GenTraversable[Polygon] = {
    val csf = geometryFactory.getCoordinateSequenceFactory
    formRings(segments.map(_.getCoordinateSequence).map(s => new VirtualCoordinateSequence(Seq(s))))
      .map(csf.create(_))
      .map(geometryFactory.createPolygon)
  }

  def dissolveRings(rings: Array[Polygon]): (Seq[Polygon], Seq[Polygon]) = {
    Option(geometryFactory.createGeometryCollection(rings.asInstanceOf[Array[Geometry]]).union) match {
      case Some(mp) =>
        val polygons = for (i <- 0 until mp.getNumGeometries) yield {
          mp.getGeometryN(i).asInstanceOf[Polygon]
        }

        (polygons.map(_.getExteriorRing.getCoordinates).map(geometryFactory.createPolygon),
         polygons.flatMap(getInteriorRings).map(geometryFactory.createPolygon))
      case None =>
        (Vector.empty[Polygon], Vector.empty[Polygon])
    }
  }

  def getInteriorRings(p: Polygon): Seq[LinearRing] =
    for (i <- 0 until p.getNumInteriorRing)
      yield geometryFactory.createLinearRing(p.getInteriorRingN(i).getCoordinates)

  class AssemblyException(msg: String) extends Exception(msg)
}
