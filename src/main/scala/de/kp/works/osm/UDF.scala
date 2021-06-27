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
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

object UDF extends Serializable {
  /**
   * This method evaluates the `tag` column of an OSM-specific
   * dataframe and evaluates whether the provided key matches
   * one of the tag keys. In case of a match, the associated
   * value is returned.
   */
  def keyMatch(key:String): UserDefinedFunction =
    udf((tags: mutable.WrappedArray[Row]) => {

      var value:String = ""
      tags.foreach(tag => {

        val k = new String(tag.getAs[Array[Byte]]("key"))
        val v = new String(tag.getAs[Array[Byte]]("value"))

        if (k ==  key) value = v

    })

    value

  })

}
