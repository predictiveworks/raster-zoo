package de.kp.works.vectorpipe.relations
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

import org.locationtech.jts.geom.CoordinateSequence

package object utils {

  /**
    * Tests whether two {@link CoordinateSequence}s are equal.
    * To be equal, the sequences must be the same length.
    * They do not need to be of the same dimension,
    * but the ordinate values for the smallest dimension of the two
    * must be equal.
    * Two <code>NaN</code> ordinates values are considered to be equal.
    *
    * Ported to Scala from JTS 1.15.0
    *
    * @param cs1 a CoordinateSequence
    * @param cs2 a CoordinateSequence
    * @return true if the sequences are equal in the common dimensions
    */
  def isEqual(cs1: CoordinateSequence, cs2: CoordinateSequence): Boolean = {
    if (cs1.size != cs2.size) {
      false
    } else {
      val dim = Math.min(cs1.getDimension, cs2.getDimension)
      (0 until cs1.size).forall(i => {
        (0 until dim).forall(d => {
          val v1 = cs1.getOrdinate(i, d)
          val v2 = cs2.getOrdinate(i, d)

          v1 == v2 || (v1 == Double.NaN && v2 == Double.NaN)
        })
      })
    }
  }

}
