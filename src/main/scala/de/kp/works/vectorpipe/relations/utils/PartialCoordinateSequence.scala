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

import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Envelope}

class PartialCoordinateSequence(sequence: CoordinateSequence, offset: Int)
    extends CoordinateSequence {
  private lazy val _size: Int = sequence.size() - offset

  private lazy val coordinates: Array[Coordinate] = {
    val coords = new Array[Coordinate](size())

    for (i <- 0 until size) {
      coords(i) = getCoordinate(i)
    }

    coords
  }

  override def getDimension: Int = sequence.getDimension

  override def getCoordinate(i: Int): Coordinate = sequence.getCoordinate(offset + i)

  override def getCoordinateCopy(i: Int): Coordinate = sequence.getCoordinateCopy(offset + i)

  override def getCoordinate(index: Int, coord: Coordinate): Unit =
    sequence.getCoordinate(offset + index, coord)

  override def getOrdinate(index: Int, ordinateIndex: Int): Double =
    sequence.getOrdinate(offset + index, ordinateIndex)

  override def setOrdinate(index: Int, ordinateIndex: Int, value: Double): Unit =
    sequence.setOrdinate(offset + index, ordinateIndex, value)

  override def toCoordinateArray: Array[Coordinate] = coordinates

  override def expandEnvelope(env: Envelope): Envelope = {
    for (i <- 0 until size) {
      env.expandToInclude(getX(i), getY(i))
    }

    env
  }

  override def getX(index: Int): Double = sequence.getX(offset + index)

  override def getY(index: Int): Double = sequence.getY(offset + index)

  override def size(): Int = _size

  override def clone(): AnyRef = new PartialCoordinateSequence(sequence, offset)

  override def copy(): PartialCoordinateSequence = new PartialCoordinateSequence(sequence.copy, offset)
}
