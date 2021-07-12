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

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType}

import de.kp.works.vectorpipe.util._

package object functions {
  // A brief note about style
  // Spark functions are typically defined using snake_case, therefore so are the UDFs
  // internal helper functions use standard Scala naming conventions

  @transient lazy val merge_counts: UserDefinedFunction = udf(_mergeCounts)

  @transient lazy val sum_counts: UserDefinedFunction = udf { counts: Iterable[Map[String, Int]] =>
    counts.reduce(_mergeCounts(_, _))
  }

  // Convert BigDecimals to doubles
  // Reduces size taken for representation at the expense of some precision loss.
  def asDouble(value: Column): Column =
    when(value.isNotNull, value.cast(DoubleType))
      .otherwise(lit(Double.NaN)) as s"asDouble($value)"

  // Convert BigDecimals to floats
  // Reduces size taken for representation at the expense of more precision loss.
  def asFloat(value: Column): Column =
    when(value.isNotNull, value.cast(FloatType))
      .otherwise(lit(Float.NaN)) as s"asFloat($value)"

  @transient lazy val count_values: UserDefinedFunction = udf {
    (_: Seq[String]).groupBy(identity).mapValues(_.size)
  }

  @transient lazy val flatten: UserDefinedFunction = udf {
    (_: Seq[Seq[String]]).flatten
  }

  @transient lazy val flatten_set: UserDefinedFunction = udf {
    (_: Seq[Seq[String]]).flatten.distinct
  }

  @transient lazy val merge_sets: UserDefinedFunction = udf { (a: Iterable[String], b: Iterable[String]) =>
    (Option(a).getOrElse(Set.empty).toSet ++ Option(b).getOrElse(Set.empty).toSet).toArray
  }

  @transient lazy val without: UserDefinedFunction = udf { (list: Seq[String], without: String) =>
    list.filterNot(x => x == without)
  }

  private val _mergeCounts = (a: Map[String, Int], b: Map[String, Int]) =>
    mergeMaps(Option(a).getOrElse(Map.empty[String, Int]),
              Option(b).getOrElse(Map.empty[String, Int]))(_ + _)
}
