package de.kp.works.model.build
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

import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.zoo.pipeline.api.keras.layers._
import com.intel.analytics.zoo.pipeline.api.keras.models._
import com.typesafe.config.{Config, ConfigObject}

object ModelBuilder extends LayerBuilder with OptimizerBuilder with LossBuilder with MetricsBuilder {

  def buildKeras(spec:String, compile:Boolean = true):Either[Sequential[Float], Throwable] = {

    try {

      val config = spec2Config(spec)
      val model = config2KerasModel(config, compile)

      Left(model)

    } catch {
      case t:Throwable =>

        val now = System.currentTimeMillis
        val date = new java.util.Date(now)

        Right(t)
    }
  }

  def config2KerasModel(config:Config, compile:Boolean):Sequential[Float] = {

    try {

      val modelConf = config.getConfig("model")
      val `type` = modelConf.getString("type")
      /*
       * Accepted model templates
       */
      if (`type`.toLowerCase != "keras") {

        val message = s"The model template '${`type`}' is not supported."
        logger.error(message)

        throw new Exception(message)

      }

      val sequential = Sequential[Float]()

      input2Model(sequential, modelConf)
      layers2Model(sequential, modelConf)

      if (!compile) return sequential

      val optimizer = try {
        config2Optimizer(modelConf.getConfig("optimizer"))

      } catch {
        case _:Throwable =>

          val message = s"The model optimizer is not specified. Cannot compile model."
          logger.warn(message)

          null
      }

      val loss = try {
        config2Loss(modelConf.getConfig("loss"))

      } catch {
        case _:Throwable =>

          val message = s"The model loss is not specified. Cannot compile model."
          logger.warn(message)

          null
      }

      val metrics = try {
        config2Metrics(modelConf.getConfig("metrics"))

      } catch {
        case _:Throwable =>

          val message = s"The model metrics is not specified."
          logger.warn(message)

          null
      }

      if (optimizer == null || loss == null) {

        val message = s"The model cannot be compile. Optimizer and/or loss are not specified."
        logger.error(message)

        throw new Exception(message)
      }

      if (metrics == null)
        sequential.compile(optimizer = optimizer, loss = loss)

      else {
        sequential.compile(optimizer = optimizer, loss = loss, metrics = metrics)
      }

      sequential

    } catch {
      case t:Throwable =>
        if (verbose) t.printStackTrace()
        throw new Exception(t.getLocalizedMessage)
    }
  }

  private def input2Model(model:Sequential[Float], modelConf:Config):Unit = {
    /*
     * Convert `input` specification into Shape first
     * to enable layer building with respect to this input
     */
    val input = try {
      modelConf.getList("input")

    } catch {
      case _:Throwable =>

        val message = s"The model input is not specified."
        logger.warn(message)

        null
    }

    if (input != null) {

      val inputLayer = InputLayer(inputShape = config2Shape(input))
      model.add(inputLayer)

    }

  }

  private def layers2Model(model:Sequential[Float], modelConf:Config):Unit = {

    val version = getAsString(modelConf, "version", "V1")

    val layers = try {
      modelConf.getList("layers")

    } catch {
      case _:Throwable =>

        val message = s"The model layers is not specified."
        logger.error(message)

        throw new Exception(message)
    }

    val size = layers.size
    for (i <- 0 until size) {

      val cval = layers.get(i)
      /*
       * Unpack layer as Config
       */
      cval match {
        case configObject: ConfigObject =>

          val layer = configObject.toConfig

          val kerasLayer = config2Layer(layer, version)
          model.add(kerasLayer)

        case _ =>
          throw new Exception(s"Layer $i is not specified as configuration object.")
      }

    }

  }

}
