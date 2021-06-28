package de.kp.works.model.cnn
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
import com.intel.analytics.zoo.pipeline.api.keras.models.Sequential
import de.kp.works.model.build.ModelBuilder

/**
 * Potential use case
 *
 * - land use classification problem
 */
object SimpleCNN {

  val template: String =
    """model {
      |  name    = "SimpleCNN",
      |  type    = "keras",
      |  version = "V2",
      |  input   = [%input1,%input2, %input3],
      |  layers  = [
      |    {
      |     name = "layer1",
      |     type = "Convolution2D",
      |     params = {
      |       activation = "%activation1",
      |       bias = %bias,
      |       filters = %filters,
      |       kernelSize= [%kernelSize1, %kernelSize2],
      |     },
      |    },
      |    {
      |     name = "layer2",
      |     type = "Flatten",
      |     params = {
      |     },
      |    },
      |    {
      |     name = "layer3",
      |     type = "Dense",
      |     params = {
      |       activation = "%activation2",
      |       units = %units,
      |     },
      |    },
      |  ],
      |  optimizer = {
      |    name = "optimizer",
      |    type = "%optimizer",
      |    params = {
      |      learningRate = %learningRate,
      |    },
      |  },
      |  loss = {
      |    name = "loss",
      |    type = "%loss",
      |    params = {
      |    },
      |  },
      |  metrics = {
      |    name = "metrics",
      |    type = "%metric",
      |    params = {
      |    },
      |  }
      |}
      |""".stripMargin

  def apply(settings:Map[String,String]):Sequential[Float] = {
    /*
     * Convolution layer
     */
    val activation1 = settings.getOrElse("activation1", "relu")
    val activation2 = settings.getOrElse("activation2", "softmax")
    val bias        = settings.getOrElse("bias",        "true")
    val filters     = settings.getOrElse("filters",     "32")
    val kernelSize1 = settings.getOrElse("kernelSize1", "3")
    val kernelSize2 = settings.getOrElse("kernelSize2", "3")
    val input1      = settings.getOrElse("input1",      "64")
    val input2      = settings.getOrElse("input2",      "64")
    val input3      = settings.getOrElse("input3",      "3")
    val units       = settings.getOrElse("units",       "10")
    /*
     * Check whether provided `optimizer` is supported
     */
    val optimizer = settings.getOrElse("optimizer", "Adam")

    if (!ModelBuilder.getOptimizers.contains(optimizer)) {
      throw new Exception(s"The optimizer '$optimizer' is not supported.")
    }

    val learningRate = settings.getOrElse("learningRate", "0.0001")

    /*
     * Check whether provided `loss` is supported
     */
    val loss = settings.getOrElse("loss", "CCE")

    if (!ModelBuilder.getLosses.contains(loss)) {
      throw new Exception(s"The loss '$loss' is not supported.")
    }

    /*
     * Check whether provided `metric` is supported
     */
    val metric = settings.getOrElse("metric", "Accuracy")

    if (!ModelBuilder.getMetrics.contains(metric)) {
      throw new Exception(s"The metric '$metric' is not supported.")
    }

    /*
     * Retrieve modelSpec from blueprint
     */
    val modelSpec = template
      .replace("%activation1", activation1)
      .replace("%activation2", activation2)
      .replace("%bias",        bias)
      .replace("%filters",     filters)
      .replace("%input1",      input1)
      .replace("%input2",      input2)
      .replace("%input3",      input3)
      .replace("%kernelSize1", kernelSize1)
      .replace("%kernelSize2", kernelSize2)
      .replace("%units",       units)
      /* Compile */
      .replace("%optimizer",    optimizer)
      .replace("%learningRate", learningRate)
      .replace("%loss",         loss)
      .replace("%metric",       metric)

    val result = ModelBuilder.buildKeras(modelSpec)
    if (result.isRight)
      throw new Exception(result.right.get.getLocalizedMessage)

    val model = result.left.get
    model

  }

}

class SimpleCNN(
  activations:Array[String] = Array("relu", "softmax"),
  bias:Boolean              = true,
  filters:Int               = 32,
  input:Array[Int]          = Array(64,64,3),
  kernelSizes:Array[Int]    = Array(3,3),
  learningRate:Double       = 0.0001,
  loss:String               = "CCE", // Cross Categorical Entropy
  metric:String             = "Accuracy",
  optimizer:String          = "Adam",
  units:Int                 = 10) {

  private val params = Map(
    "activation1"  -> activations(0),
    "activation2"  -> activations(1),
    "bias"         -> bias.toString,
    "filters"      -> filters.toString,
    "input1"       -> input(0).toString,
    "input2"       -> input(1).toString,
    "input3"       -> input(2).toString,
    "kernelSize1"  -> kernelSizes(0).toString,
    "kernelSize2"  -> kernelSizes(1).toString,
    "learningRate" -> learningRate.toString,
    "loss"         -> loss,
    "metric"       -> metric,
    "optimizer"    -> optimizer,
    "units"        -> units.toString)

  def getModel: Sequential[Float] = SimpleCNN(params)

}