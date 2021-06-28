package de.kp.works.model.build
/*+
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

import com.intel.analytics.bigdl.optim.ValidationMethod
import com.typesafe.config.Config
import com.intel.analytics.zoo.pipeline.api.keras.metrics._
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat

trait MetricsBuilder extends SpecBuilder {

  def getMetrics = List(
    "Accuracy",
    "BinaryAccuracy",
    "CategoricalAccuracy",
    "MAE",
    "MSE",
    "SparseCategoricalAccuracy",
    "Top5Accuracy")

  def config2Metrics(metrics:Config): List[ValidationMethod[Float]] = {

    val params = metrics.getConfig("params")

    metrics.getString("type") match {
      case "Accuracy" =>

        /* This metrics is deprecated */
        logger.warn("Metrics 'Accuracy' is deprecated. Use 'SparseCategoricalAccuracy', "
          + "'CategoricalAccuracy' or 'BinaryAccuracy' instead")

        /*
         * Whether target labels start from 0. Default is true.
         * If false, labels start from 1. Note that this only
         * takes effect for multi-class classification.
         *
         * For binary classification, labels ought to be 0 or 1.
         */
        val zeroBasedLabel = getAsBoolean(params, "zeroBasedLabel", default = true)
        List(new Accuracy(zeroBasedLabel = zeroBasedLabel))

      case "BinaryAccuracy" =>
        /*
         * Measures top1 accuracy for binary classification
         * with zero-base index.
         */
        List(new BinaryAccuracy())
      case "CategoricalAccuracy" =>
        /*
         * Measures top1 accuracy for multi-class with "one-hot"
         * target.
         */
        List(new CategoricalAccuracy())

      case "MAE" => List(new MAE[Float]())
      case "MSE" => List(new MSE[Float]())

      case "SparseCategoricalAccuracy" =>
        /*
         * Measures top1 accuracy for multi-class classification
         * with sparse target and zero-base index.
         */
        List(new SparseCategoricalAccuracy())

      case "Top5Accuracy" =>
        /*
         * Measures top5 accuracy for multi-class classification.
         */

        /*
         * Whether target labels start from 0. Default is true.
         * If false, labels start from 1. Note that this only
         * takes effect for multi-class classification.
         *
         * For binary classification, labels ought to be 0 or 1.
         */
        val zeroBasedLabel = getAsBoolean(params, "zeroBasedLabel", default = true)
        List(new Top5Accuracy(zeroBasedLabel = zeroBasedLabel))

      case _ => null
    }
  }
}
