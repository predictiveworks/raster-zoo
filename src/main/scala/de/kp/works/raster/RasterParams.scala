package de.kp.works.raster
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

trait RasterParams {
  /*
   * An internal name to specify the name of a temporary
   * column that contains the dimension of a tile.
   */
  protected val dimensionColName = "_dimension"
  /*
   * Tile dimension is specified leveraging two columns,
   * for width and height
   */
  protected val heightColName = "height"
  protected val widthColName  = "width"
  /*
   * An internal name to specify the name of a temporary
   * column that contains the extent of a tile.
   */
  protected val extentColName = "_extent"
  /*
   * An internal name to specify the name of a temporary
   * column that contains the geometry of a tile.
   */
  protected val geometryColName:String = "_geometry"
  /*
   * Raster-Zoo assigns an index to every row
   * of a rasterframe.
   *
   * The `index` column enables proper transformation
   * and re-transformation with respect to Analytics-Zoo
   * RDD representations.
   */
  protected var indexColName:String  = "index"
  /*
   * The name of the column that contains single band
   * tiles, which build the basis annotation and indexing.
   */
  protected var rasterColName:String = "proj_raster"
  /*
   * An name to specify the column that contains
   * the tile's resolution.
   */
  protected val resolutionColName  = "resolution"
 /*
   * An internal name to specify the temporary column
   * that contains the units of measure of a tile.
   */
  protected val unitsColName = "_units"

  protected var resolution:Int = -1

  protected def setIndexColName(name:String):Unit = {
    indexColName = name
  }

  protected def setRasterColName(name:String):Unit = {
    rasterColName = name
  }

  protected def setResolutionValue(value:Int):Unit = {
    resolution = value
  }
}
