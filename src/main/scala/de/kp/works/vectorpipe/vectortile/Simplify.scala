package de.kp.works.vectorpipe.vectortile
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

import geotrellis.vector._
import geotrellis.layer._

import org.locationtech.jts.simplify.TopologyPreservingSimplifier

object Simplify {

  /**
   * Simplifies geometry using JTS's topology-preserving simplifier.
   *
   * Note that there are known bugs with this simplifier.  Please refer to the
   * JTS documentation.  Faster simplifiers with fewer guarantees are available
   * there as well.
   */
  def withJTS(g: Geometry, ld: LayoutDefinition): Geometry = {
    TopologyPreservingSimplifier.simplify(g, ld.cellSize.resolution)
  }

}
