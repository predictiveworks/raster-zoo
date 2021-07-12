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

class ReversedCoordinateSequence(sequence: CoordinateSequence) extends CoordinateSequence {
  private lazy val coordinates: Array[Coordinate] = {
    val coords = new Array[Coordinate](size())

    for (i <- size - 1 to 0) {
      coords(i) = getCoordinate(i)
    }

    coords
  }

  override def getDimension: Int = sequence.getDimension

  override def getCoordinate(i: Int): Coordinate = sequence.getCoordinate(getIndex(i))

  override def getCoordinateCopy(i: Int): Coordinate = sequence.getCoordinateCopy(getIndex(i))

  override def getCoordinate(index: Int, coord: Coordinate): Unit =
    sequence.getCoordinate(getIndex(index), coord)

  private def getIndex(i: Int): Int = size - 1 - i

  override def size(): Int = sequence.size

  override def getX(index: Int): Double = sequence.getX(getIndex(index))

  override def getY(index: Int): Double = sequence.getY(getIndex(index))

  override def getOrdinate(index: Int, ordinateIndex: Int): Double =
    sequence.getOrdinate(getIndex(index), ordinateIndex)

  override def setOrdinate(index: Int, ordinateIndex: Int, value: Double): Unit =
    sequence.setOrdinate(getIndex(index), ordinateIndex, value)

  override def toCoordinateArray: Array[Coordinate] = coordinates

  override def expandEnvelope(env: Envelope): Envelope = sequence.expandEnvelope(env)

  override def clone(): AnyRef = new ReversedCoordinateSequence(sequence)

  override def copy(): ReversedCoordinateSequence = new ReversedCoordinateSequence(sequence.copy)
}
