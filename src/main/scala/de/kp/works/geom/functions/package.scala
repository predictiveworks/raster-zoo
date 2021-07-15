package de.kp.works.geom
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

import de.kp.works.geom.model.BBox
import org.locationtech.jts.geom.{Geometry => JtsGeometry}

package object functions {

  def geometryToBBox(geometry: JtsGeometry): BBox = {

    val envelope = geometry.getEnvelope
    val coordinates = envelope.getCoordinates

    if (coordinates.isEmpty)
      BBox()

    else {
      /* Longitude refers to the X coordinate */
      val (minLon, maxLon) = {
        val xs = coordinates.map(_.x)
        (xs.min, xs.max)
      }
      /* Latitude refers to the Y coordinate */
      val (minLat, maxLat) = {
        val ys = coordinates.map(_.y)
        (ys.min, ys.max)
      }

      BBox(minLon = minLon, minLat = minLat, maxLon = maxLon, maxLat = maxLat)

    }

  }

  def envelopeToBoundary(geometry: JtsGeometry): JtsGeometry = {
    geometry.getEnvelope
  }

}