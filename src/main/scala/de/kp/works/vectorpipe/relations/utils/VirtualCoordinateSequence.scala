package de.kp.works.vectorpipe.relations.utils
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

import com.google.common.collect.{Range, RangeMap, TreeRangeMap}
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Envelope}

// rather than being a nested set of CoordinateSequences, this is a mutable wrapper to avoid deep call stacks
class VirtualCoordinateSequence(sequences: Seq[CoordinateSequence]) extends CoordinateSequence {
  // TODO this should be invalidated after append (but it doesn't actually matter because all of the appending will
  // occur ahead of time)
  private lazy val coordinates: Array[Coordinate] = {
    val coords = new Array[Coordinate](size())

    for (i <- 0 until size) {
      coords(i) = getCoordinate(i)
    }

    coords
  }

  private val rangeMap: RangeMap[Integer, CoordinateSequence] = {
    val rm = TreeRangeMap.create[Integer, CoordinateSequence]

    sequences
      .zip(sequences.map(_.size).scanLeft(0)(_ + _).dropRight(1))
      .map {
        case (seq, offset) => (seq, Range.closed(offset: Integer, offset + seq.size - 1: Integer))
      }
      .foreach { case (seq, range) => rm.put(range, seq) }

    rm
  }

  private var dimension: Int = sequences.map(_.getDimension).min

  private var _size: Int = sequences.map(_.size).sum

  def append(sequence: CoordinateSequence): VirtualCoordinateSequence = {
    val upperEndpoint = rangeMap.span.upperEndpoint
    val range = Range.closed(upperEndpoint + 1: Integer, upperEndpoint + sequence.size: Integer)
    rangeMap.put(range, sequence)

    dimension = Math.min(dimension, sequence.getDimension)
    _size += sequence.size

    this
  }

  override def getDimension: Int = dimension

  override def getCoordinate(i: Int): Coordinate = {
    val (sequence, index) = getSequence(i)

    // bypass PackedCoordinateSequence.getCoordinate to prevent caching and associated allocation
    new Coordinate(sequence.getX(index), sequence.getY(index))
  }

  private def getSequence(i: Int): (CoordinateSequence, Int) = {
    val entry = rangeMap.getEntry(i: Integer)

    (entry.getValue, i - entry.getKey.lowerEndpoint)
  }

  override def getCoordinateCopy(i: Int): Coordinate = {
    val (sequence, index) = getSequence(i)

    sequence.getCoordinateCopy(index)
  }

  override def getCoordinate(i: Int, coord: Coordinate): Unit = {
    val (sequence, index) = getSequence(i)

    sequence.getCoordinate(index, coord)
  }

  override def getOrdinate(i: Int, ordinateIndex: Int): Double = {
    val (sequence, index) = getSequence(i)

    sequence.getOrdinate(index, ordinateIndex)
  }

  override def setOrdinate(i: Int, ordinateIndex: Int, value: Double): Unit = {
    val (sequence, index) = getSequence(i)

    sequence.setOrdinate(index, ordinateIndex, value)
  }

  override def toCoordinateArray: Array[Coordinate] = coordinates

  override def expandEnvelope(env: Envelope): Envelope = {
    for (i <- 0 until size) {
      env.expandToInclude(getX(i), getY(i))
    }

    env
  }

  override def getX(i: Int): Double = {
    val (sequence, index) = getSequence(i)

    sequence.getX(index)
  }

  override def getY(i: Int): Double = {
    val (sequence, index) = getSequence(i)

    sequence.getY(index)
  }

  override def size(): Int = _size

  override def clone(): AnyRef = {
    // we're already playing fast and loose
    this
  }

  override def copy(): VirtualCoordinateSequence = new VirtualCoordinateSequence(sequences.map(_.copy))
}
