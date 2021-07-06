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
 * Purpose:
 *
 * Classify roads and features in satellite imagery.
 */
object DeepOSM {
  /**
   * The architecture of the supported NN mirror chapter 3 of
   * www.cs.toronto.edu/~vmnih/docs/Mnih_Volodymyr_PhD_Thesis.pdf
   *
   * ONE_LAYER_RELU
   *
   * network = tflearn.fully_connected(network, 64, activation='relu')
   * softmax = tflearn.fully_connected(network, 2, activation='softmax')
   *
   * ONE_LAYER_RELU_CONV
   *
   * network = conv_2d(network, 64, 12, strides=4, activation='relu')
   * network = max_pool_2d(network, 3)
   * softmax = tflearn.fully_connected(network, 2, activation='softmax')
   *
   * TWO_LAYER_RELU_CONV
   *
   *  network = conv_2d(network, 64, 12, strides=4, activation='relu')
   *  network = max_pool_2d(network, 3)
   *  network = conv_2d(network, 128, 4, activation='relu')
   *  softmax = tflearn.fully_connected(network, 2, activation='softmax')
   *
   * The NN models are used to classify images whether they contain
   * roads or not.
   *
   * The original implementation of the NNs is based on the high-level
   * library TFLearn and leverages a Momentum optimizer, which translates
   * into Keras `SGD`.
   *
   * The hyper parameters specified for the SGD (or Momentum) optimizer
   * also mirror the settings of the above referenced thesis.
   */
  val ONE_LAYER_RELU: String =
    """model {
      |  name    = "OneLayerRelu",
      |  type    = "keras",
      |  version = "V2",
      |  input   = [%input1,%input2, %input3],
      |  layers  = [
      |    {
      |     name = "layer1",
      |     type = "Dense",
      |     params = {
      |       activation = "%activation1",
      |       units      = %units1,
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
      |       activation = "%activation3",
      |       units      = %units2,
      |     },
      |    },
      |  ],
      |  optimizer = {
      |    name = "optimizer",
      |    type = "%optimizer",
      |    params = {
      |      decayRate    = %decayRate,
      |      learningRate = %learningRate,
      |      momentum     = %momentum,
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

  val ONE_LAYER_RELU_CONV: String =
    """model {
      |  name    = "OneLayerReluConv",
      |  type    = "keras",
      |  version = "V2",
      |  input   = [%input1,%input2, %input3],
      |  layers  = [
      |    {
      |     name = "layer1",
      |     type = "Convolution2D",
      |     params = {
      |       activation = "%activation1",
      |       filters    = %filters1,
      |       kernelSize = [%kernelSize1,%kernelSize1],
      |       strides    = [%strides1,%strides1]
      |     },
      |    },
      |    {
      |     name = "layer2",
      |     type = "MaxPooling2D",
      |     params = {
      |       poolSize = [%poolSize, %poolSize],
      |     },
      |    },
      |    {
      |     name = "layer3",
      |     type = "Flatten",
      |     params = {
      |     },
      |    },
      |    {
      |     name = "layer4",
      |     type = "Dense",
      |     params = {
      |       activation = "%activation3",
      |       units      = %units2,
      |     },
      |    },
      |  ],
      |  optimizer = {
      |    name = "optimizer",
      |    type = "%optimizer",
      |    params = {
      |      decayRate    = %decayRate,
      |      learningRate = %learningRate,
      |      momentum     = %momentum,
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

  val TWO_LAYER_RELU_CONV: String =
    """model {
      |  name    = "OneLayerReluConv",
      |  type    = "keras",
      |  version = "V2",
      |  input   = [%input1,%input2, %input3],
      |  layers  = [
      |    {
      |     name = "layer1",
      |     type = "Convolution2D",
      |     params = {
      |       activation = "%activation1",
      |       filters    = %filters1,
      |       kernelSize = [%kernelSize1,%kernelSize1],
      |       strides    = [%strides1,%strides1]
      |     },
      |    },
      |    {
      |     name = "layer2",
      |     type = "MaxPooling2D",
      |     params = {
      |       poolSize = [%poolSize, %poolSize],
      |     },
      |    },
      |    {
      |     name = "layer3",
      |     type = "Convolution2D",
      |     params = {
      |       activation = "%activation2",
      |       filters    = %filters2,
      |       kernelSize = [%kernelSize2,%kernelSize2],
      |       strides    = [%strides2,%strides2]
      |     },
      |    },
      |    {
      |     name = "layer4",
      |     type = "Flatten",
      |     params = {
      |     },
      |    },
      |    {
      |     name = "layer5",
      |     type = "Dense",
      |     params = {
      |       activation = "%activation3",
      |       units      = %units2,
      |     },
      |    },
      |  ],
      |  optimizer = {
      |    name = "optimizer",
      |    type = "%optimizer",
      |    params = {
      |      decayRate    = %decayRate,
      |      learningRate = %learningRate,
      |      momentum     = %momentum,
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

    val modelType = settings("modelType")

    val activation1 = settings.getOrElse("activation1", "relu")
    val activation2 = settings.getOrElse("activation2", "relu")
    val activation3 = settings.getOrElse("activation3", "softmax")
    val filters1    = settings.getOrElse("filters1",    "64")
    val filters2    = settings.getOrElse("filters2",    "128")
    val input1      = settings.getOrElse("input1",      "64")
    val input2      = settings.getOrElse("input2",      "64")
    val input3      = settings.getOrElse("input3",      "3")
    val kernelSize1 = settings.getOrElse("kernelSize1", "12")
    val kernelSize2 = settings.getOrElse("kernelSize2", "4")
    val poolSize    = settings.getOrElse("poolSize",    "3")
    val strides1    = settings.getOrElse("strides1",    "4")
    val strides2    = settings.getOrElse("strides2",    "1")
    val units1      = settings.getOrElse("units1",      "64")
    val units2      = settings.getOrElse("units2",      "2")

    /*
     * Check whether provided `optimizer` is supported
     */
    val optimizer = settings.getOrElse("optimizer", "SGD")

    if (!ModelBuilder.getOptimizers.contains(optimizer)) {
      throw new Exception(s"The optimizer '$optimizer' is not supported.")
    }

    val decayRate    = settings.getOrElse("decayRate", "0.0002")
    val learningRate = settings.getOrElse("learningRate", "0.005")
    val momentum     = settings.getOrElse("momentum", "0.9")

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
    val modelSpec = modelType match {
      case ONE_LAYER_RELU =>
        ONE_LAYER_RELU
          .replace("%activation1",  activation1)
          .replace("%activation3",  activation3)
          .replace("%decayRate",    decayRate)
          .replace("%input1",       input1)
          .replace("%input2",       input2)
          .replace("%input3",       input3)
          .replace("%learningRate", learningRate)
          .replace("%loss",         loss)
          .replace("%metric",       metric)
          .replace("%momentum",     momentum)
          .replace("%optimizer",    optimizer)
          .replace("%units1",       units1)

      case ONE_LAYER_RELU_CONV =>
        ONE_LAYER_RELU_CONV
          .replace("%activation1",  activation1)
          .replace("%activation3",  activation3)
          .replace("%decayRate",    decayRate)
          .replace("%filters1",     filters1)
          .replace("%input1",       input1)
          .replace("%input2",       input2)
          .replace("%input3",       input3)
          .replace("%kernelSize1",  kernelSize1)
          .replace("%learningRate", learningRate)
          .replace("%loss",         loss)
          .replace("%metric",       metric)
          .replace("%momentum",     momentum)
          .replace("%optimizer",    optimizer)
          .replace("%poolSize",     poolSize)
          .replace("%strides1",     strides1)
          .replace("%units2",       units2)

      case TWO_LAYER_RELU_CONV =>
        TWO_LAYER_RELU_CONV
          .replace("%activation1",  activation1)
          .replace("%activation2",  activation2)
          .replace("%activation3",  activation3)
          .replace("%decayRate",    decayRate)
          .replace("%filters1",     filters1)
          .replace("%filters2",     filters2)
          .replace("%input1",       input1)
          .replace("%input2",       input2)
          .replace("%input3",       input3)
          .replace("%kernelSize1",  kernelSize1)
          .replace("%kernelSize2",  kernelSize2)
          .replace("%learningRate", learningRate)
          .replace("%loss",         loss)
          .replace("%metric",       metric)
          .replace("%momentum",     momentum)
          .replace("%optimizer",    optimizer)
          .replace("%poolSize",     poolSize)
          .replace("%strides1",     strides1)
          .replace("%strides2",     strides2)
          .replace("%units2",       units2)

      case _ => throw new Exception(s"Model type `$modelType` not supported.")
    }

    val result = ModelBuilder.buildKeras(modelSpec)
    if (result.isRight)
      throw new Exception(result.right.get.getLocalizedMessage)

    val model = result.left.get
    model

 }

}

/**
 * DeepOSM supports three different image classification
 * models, extracted from the PhD thesis of Mnih, Volodymyr.
 */
class DeepOSM(modelType:String,
   activations:Array[String] = Array("relu", "relu", "softmax"),
   decayRate:Double          = 0.0002,
   filters:Array[Int]        = Array(64, 128),
   input:Array[Int]          = Array(64,64,3),
   kernelSizes:Array[Int]    = Array(12, 4),
   learningRate:Double       = 0.005,
   loss:String               = "CCE", // Cross Categorical Entropy
   metric:String             = "Accuracy",
   momentum:Double           = 0.9,
   optimizer:String          = "SGD",
   poolSize:Int              = 3,
   strides:Array[Int]        = Array(4, 1),
   units:Array[Int]          = Array(64,2)) {

  private val params = Map(
    "modelType"    -> modelType,
    "activation1"  -> activations(0),
    "activation2"  -> activations(1),
    "activation3"  -> activations(2),
    "decayRate"    -> decayRate.toString,
    "filters1"     -> filters(0).toString,
    "filters2"     -> filters(1).toString,
    "input1"       -> input(0).toString,
    "input2"       -> input(1).toString,
    "input3"       -> input(2).toString,
    "kernelSize1"  -> kernelSizes(0).toString,
    "kernelSize2"  -> kernelSizes(1).toString,
    "learningRate" -> learningRate.toString,
    "loss"         -> loss,
    "metric"       -> metric,
    "momentum"     -> momentum.toString,
    "optimizer"    -> optimizer,
    "poolSize"     -> poolSize.toString,
    "strides1"     -> strides(0).toString,
    "strides2"     -> strides(1).toString,
    "units1"       -> units(0).toString,
    "units2"       -> units(1).toString)

  def getModel: Sequential[Float] = DeepOSM(params)

}
