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

import com.intel.analytics.bigdl.nn.abstractnn.Activity
import com.intel.analytics.bigdl.nn.keras.KerasLayer
import com.intel.analytics.bigdl.optim.L1L2Regularizer
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.layers.{ELU, Flatten}
import com.intel.analytics.zoo.pipeline.api.{keras, keras2}
import com.intel.analytics.zoo.pipeline.api.keras2.layers
import com.intel.analytics.zoo.pipeline.api.net.GraphNet
import com.typesafe.config.{Config, ConfigList}

trait LayerBuilder extends SpecBuilder with OptimizerBuilder {
  /*
   * The list of supported model types for pre-trained models
   */
  private val modelTypes = Array("analytics_zoo", "big_dl", "caffe", "torch")
  /*
   * The (internal & optionally local) folder that
   * is used as the base folder for pre-trained deep
   * learning models
   */
  private val modelBase = DLConfig.getFolder

  def config2Layer(config:Config, kerasVersion:String) = {

    val bidirectional   = getAsBoolean(config, "biDirectional", default = false)
    val timeDistributed = getAsBoolean(config, "timeDistributed", default = false)

    val kerasLayer = config.getString("type") match {

      /* V1 & V2 */
      case "AveragePooling1D" =>
        if (kerasVersion == "V1")
          config2AvgPool1D_V1(config)

        else
          config2AvgPool1D_V2(config)
      case "BatchNormalization" =>
        config2BatchNorm(config)
      /* V1 & V2 */
      case "Convolution1D" =>
        if (kerasVersion == "V1")
          config2Conv1D_V1(config)

        else
          config2Conv1D_V2(config)
      /* V1 & V2 */
      case "Convolution2D" =>
        if (kerasVersion == "V1")
          config2Conv2D_V1(config)

        else
          config2Conv2D_V2(config)
      case "ConvLSTM2D" =>
        config2ConvLSTM2D(config)
      /* V1 & V2 */
      case "Dense" =>
        if (kerasVersion == "V1") {
          val dense = config2Dense_V1(config)
          if (timeDistributed)
            keras.layers.TimeDistributed(dense.asInstanceOf[KerasLayer[Activity, Tensor[Float], Float]])
          else
            dense
        }
        else {
          val dense = config2Dense_V2(config)
          if (timeDistributed)
            keras.layers.TimeDistributed(dense.asInstanceOf[KerasLayer[Activity, Tensor[Float], Float]])
          else
            dense
        }
      /* V1 & V2 */
      case "Dropout" =>
        if (kerasVersion == "V1")
          config2Dropout_V1(config)

        else
          config2Dropout_V2(config)
      case "ELU" =>
        config2ELU(config)
      /* V1 & V2 */
      case "Flatten" =>
        if (kerasVersion == "V1")
          config2Flatten_V1(config)

        else
          config2Flatten_V2(config)
      /* V1 & V2 */
      case "GlobalAveragePooling1D" =>
        if (kerasVersion == "V1")
          config2GlobalAvgPool1D_V1(config)

        else
          config2GlobalAvgPool1D_V2(config)
      /* V1 & V2 */
      case "GlobalMaxPooling1D" =>
        if (kerasVersion == "V1")
          config2GlobalMaxPool1D_V1(config)

        else
          config2GlobalMaxPool1D_V2(config)
      case "GRU" =>
        config2GRU(config)
      case "LSTM" =>
        val lstm = config2LSTM(config)
        if (bidirectional)
        /*
         * The current implementation supports the
         * default `mergeMode` = `concat`.
         */
          keras.layers.Bidirectional(lstm)

        else
          lstm
      /* V1 & V2 */
      case "MaxPooling1D" =>
        if (kerasVersion == "V1")
          config2MaxPooling1D_V1(config)

        else
          config2MaxPooling1D_V2(config)
      case "MaxPooling2D" =>
        config2MaxPooling2D(config)
      case "Pretrained" =>
        config2Pretrained(config)
      case "RepeatVector" =>
        config2RepeatVector(config)
    }

    val name = config.getString("name")
    kerasLayer.setName(name)

  }

  /***** PRETRAINED *****/

  def config2Pretrained(layer:Config):KerasLayer[Activity, Activity, Float] = {

    val params = layer.getConfig("params")

    val modelType = getStringParam(params, "modelType").get
    if (!modelTypes.contains(modelType)) {

      val message = s"The model type '$modelType' is not supported."
      logger.error(message)

      throw new Exception(message)

    }

    val modelPath = getModelPath(params)

    val defPath = getStringParam(params, "defPath")
    val weightPath = getStringParam(params, "weightPath")


    val loader = new LoaderUtils()
    try {
      modelType match {
        case "analytics_zoo" =>

          val model = loader.load(modelType = modelType, modelPath = modelPath, weightPath = weightPath)
          //val kerasModel = truncateKerasNet(baseConf, model.left.get)

          throw new Exception("Not supported yet")
        case "big_dl" =>

          val model = loader.load(modelType = modelType, modelPath = modelPath, weightPath = weightPath)

          val truncNet = truncateGraphNet(model.right.get, layer)
          val freezeNet = freezeGraphNet(truncNet, layer)

          freezeNet.toKeras()
        case "caffe" =>

          val model = loader.load(modelType = modelType, modelPath = modelPath, defPath = defPath)

          val truncNet = truncateGraphNet(model.right.get, layer)
          val freezeNet = freezeGraphNet(truncNet, layer)

          freezeNet.toKeras()
        case "torch" =>

          val model = loader.load(modelType = modelType, modelPath = modelPath)

          val truncNet: GraphNet[Float] = truncateGraphNet(model.right.get, layer)
          val freezeNet = freezeGraphNet(truncNet, layer)

          freezeNet.toKeras()
        case _ => throw new Exception(s"Model type '$modelType' is not supported.")
      }

    } catch {
      case t:Throwable =>
        t.printStackTrace()
        val message = s"Loading pre-trained model failed with: ${t.getLocalizedMessage}"
        throw new Exception(message)
    }

  }

  def getModelPath(params:Config):String = {

    val modelPath = getStringParam(params, "modelPath")
    if (modelPath.isDefined) return modelPath.get

    /*
     * Retrieve model path from name and version
     */
    val modelName = getStringParam(params, "modelName")
    val modelVersion = getStringParam(params, "modelVersion")

    if (modelName.isEmpty || modelVersion.isEmpty)
      throw new Exception(s"Model specification does not provided enough parameters to determine model location.")

    val name = modelName.get
    val version = modelVersion.get

    val fileName = s"analytics-zoo_${name}_imagenet_$version.model"
    s"$modelBase/$fileName"

  }

  /*
   * Freeze means that the respective layers will not be trained
   */
  def freezeGraphNet(graphNet:GraphNet[Float], layer:Config):GraphNet[Float] = {

    val freeze = getFreeze(layer)
    val newGraphNet = if (freeze != null) {

      val layers = graphNet.getSubModules().map(_.getName)
      if (layers.contains(freeze)) {

        val freezeNet = graphNet.freezeUpTo(freeze)
        /*
         * 'freeze' does not mean that the respective modules
         * are not trainable; therefore the layers are set to
         * training = false as well
         */
        val upTo = layers.zipWithIndex.filter { case (name, _) => name == freeze }.head._2
        freezeNet.getSubModules().zipWithIndex.foreach{case(module, index) =>
          if (index <= upTo) module.evaluate
        }

        freezeNet

      } else
        throw new Exception(s"The pre-trained model does not contain '$freeze'.")

    } else graphNet


    newGraphNet

  }

  def truncateGraphNet(graphNet:GraphNet[Float], layer:Config):GraphNet[Float] = {

    /*
     * Evaluate whether the base model must be truncated,
     * i.e. a certain layer must be defined as output
     */
    val output = getOutput(layer)
    val newGraphNet = if (output != null) {

      val layers = graphNet.getSubModules().map(_.getName)
      if (layers.contains(output)) {
        graphNet.newGraph(output)

      } else {
        throw new Exception(s"The pre-trained model does not contain '$output'.")
      }

    } else graphNet


    newGraphNet

  }

  def getFreeze(layer:Config):String = {

    try {
      layer.getString("freeze")

    } catch {
      case t:Throwable => null
    }

  }

  def getOutput(layer:Config):String = {

    try {
      layer.getString("output")

    } catch {
      case t:Throwable => null
    }

  }

  def getStringParam(params:Config, name:String):Option[String] = {

    try {

      val param = params.getString(name)
      if (param.isEmpty) None else Option(param)

    } catch {
      case t:Throwable => None
    }

  }

  /***** BASE LAYER *****/

  /*
   * Average Pooling 1D layer
   *
   * Applies average pooling operation for temporal data.
   */
  def config2AvgPool1D_V1(layer:Config): keras.layers.AveragePooling1D[Float] = {

    val params = layer.getConfig("params")
    /*
     * Size of the region to which average pooling is applied.
     * Default is 2.
     */
    val poolLength = getAsInt(params, "poolLength", 2)
    /*
     * Factor by which to downscale. Positive integer, or -1.
     *
     * 2 will halve the input. If -1, it will default to poolLength.
     * Default is -1, and in this case it will be equal to poolSize.
     */
    val stride = getAsInt(params, "stride", -1)
    /*
     * `borderMode` is either `valid` (default) or `same`.
     */
    val borderMode = getAsString(params, "borderMode", "valid")

    keras.layers.AveragePooling1D(
      poolLength = poolLength,
      stride = stride,
      borderMode = borderMode)

  }

  def config2AvgPool1D_V2(layer:Config): keras2.layers.AveragePooling1D[Float] = {

    val params = layer.getConfig("params")
    /*
     * Size of the region to which average pooling is applied.
     * Default is 2.
     */
    val poolSize = getAsInt(params, "poolSize", 2)
    /*
     * Factor by which to downscale. Positive integer, or -1.
     *
     * 2 will halve the input. If -1, it will default to poolLength.
     * Default is -1, and in this case it will be equal to poolSize.
     */
    val strides = getAsInt(params, "strides", -1)
    /*
     * `padding` is either `valid` (default) or `same`.
     */
    val padding = getAsString(params, "padding", "valid")

    keras2.layers.AveragePooling1D(
      poolSize = poolSize,
      strides = strides,
      padding = padding)

  }
  /*
   * Batch normalization layer
   *
 	 * Normalize the activations of the previous layer at each batch,
   * i.e. applies a transformation that maintains the mean activation
   * close to 0 and the activation standard deviation close to 1.
   *
   * It is a feature-wise normalization, each feature map in the input
   * will be normalized separately.
   *
   * The input of this layer should be 4D.
 	 *
   * When you use this layer as the first layer of a model, you need to
   * provide the argument inputShape (a Single Shape, does not include
   * the batch dimension).
   */
  def config2BatchNorm(layer:Config): keras.layers.BatchNormalization[Float] = {
    /*
     * The current implementation does not support
     * any parameters
     */
    keras.layers.BatchNormalization()
  }

  def config2Conv1D_V1(layer:Config): keras.layers.Convolution1D[Float] = {

    val params = layer.getConfig("params")

    /* Number of convolution filters to use. */
    val nbFilter:Int = params.getInt("nbFilter")

    /* The extension (spatial or temporal) of each filter. */
    val filterLength:Int = params.getInt("filterLength")

    /* Activation function to use. */
    val activation:String = params.getString("activation")

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val borderMode = getAsString(params, "borderMode", "valid")

    /* Factor by which to subsample output. Integer. Default is 1.
     *
     * This parameter is also called `strides`.
     */
    val subsampleLength = getAsInt(params, "subsampleLength", 1)

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     *
     * NOTE: This is the kernel regularizer in Keras2
     */
    val wRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = params2RegularizerB(params)
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, "bias", default = true)

    keras.layers.Convolution1D(
      nbFilter = nbFilter,
      filterLength = filterLength,
      activation = activation,
      borderMode = borderMode,
      subsampleLength = subsampleLength,
      wRegularizer = wRegularizer,
      bRegularizer = bRegularizer,
      bias = bias)

  }

  def config2Conv1D_V2(layer:Config): keras2.layers.Conv1D[Float] = {

    val params = layer.getConfig("params")
    /*
     * The dimensionality of the output space
     */
    val filters = params.getInt("filters")
    /*
     * An integer specifying the length of the 1D convolution
     * window
     */
    val kernelSize = params.getInt("kernelSize")
    /*
     * An integer specifying the stride length of the convolution.
     * Specifying any stride value != 1 is incompatible with specifying
     * any `dilation_rate` value != 1.
     */
    val strides = params.getInt("strides")

    /* Activation function to use. Default is null. You can also pass in
     * corresponding string representations such as 'relu' or 'sigmoid',
     * etc. for simple activations in the factory method.
     */
    val activation:String = params.getString("activation")
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, "bias", default = true)
    /*
     * Initializer for the `kernel` weights matrix.
     */
    val kernelInitializer = getAsString(params, "kernelInitializer", "glorot_uniform")
    /*
     * Initializer for the bias vector.
     */
    val biasInitializer = getAsString(params,"biasInitializer", "zero")
    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     *
     * NOTE: This is the kernel regularizer in Keras2
     */
    val kernelRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val biasRegularizer = params2RegularizerB(params)

    /*
     * Regularizer function applied to the bias vector. Default is null.
     */
    keras2.layers.Conv1D(
      filters = filters,
      kernelSize = kernelSize,
      strides = strides,
      /*
       * `padding` is a value of `valid`, `causal` or `same`. `valid` means
       * no padding (default), while `same` results in padding the input such
       * that output has the same length as the original input.
       *
       * `causual` results in causal (dilated) convolutions, e.g. output[t]
       * does not depend on input[t+1:]. Useful when modeling temporal data
       * where the model should not violate the temporal order.
       */
      padding = "valid",
      activation = activation,
      useBias = bias,
      kernelInitializer = kernelInitializer,
      biasInitializer = biasInitializer,
      kernelRegularizer = kernelRegularizer,
      biasRegularizer = biasRegularizer)

  }

  def config2Conv2D_V1(layer:Config): keras.layers.Convolution2D[Float] = {

    val params = layer.getConfig("params")

    /* Number of convolution filters to use. */
    val nbFilter:Int = params.getInt("nbFilter")

    /* Number of rows in the convolution kernel. */
    val nbRow:Int = params.getInt("nbRow")

    /*  Number of columns in the convolution kernel. */
    val nbCol:Int = params.getInt("nbCol")

    /* Activation function to use. */
    val activation:String = params.getString("activation")

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val borderMode = getAsString(params, "borderMode", "valid")

    /*
     * Int array of length 2 corresponding to the step of
     * the convolution in the height and width dimension.
     * Also called strides elsewhere. Default is (1, 1).
     */
    val subsample = try {

      val array = getAsIntArray(params.getList("subsample"))
      (array(0), array(1))

    } catch {
      case t:Throwable => (1, 1)
    }
    /*
     * Format of input data. Either DataFormat.NCHW (dimOrdering='th') or
 		 * DataFormat.NHWC (dimOrdering='tf'). Default is NCHW.
     */
    val dimOrdering = getAsString(params, "dimOrdering", "th")

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val wRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = params2RegularizerB(params)
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, "bias", default = true)

    keras.layers.Convolution2D(
      nbFilter = nbFilter,
      nbRow = nbRow,
      nbCol = nbCol,
      activation = activation,
      borderMode = borderMode,
      subsample = subsample,
      dimOrdering = dimOrdering,
      wRegularizer = wRegularizer,
      bRegularizer = bRegularizer,
      bias = bias)

  }

  /**
   * 2D convolution layer (e.g. spatial convolution over images).
   *
   * This layer creates a convolution kernel that is convolved
   * with the layer input to produce a tensor of outputs.
   *
   * If `use_bias` is True, a bias vector is created and added
   * to the outputs. Finally, if `activation` is not `None`, it
   * is applied to the outputs as well.
   *
   * Input shape
   *
   * 4D tensor with shape:
   * `(samples, channels, rows, cols)` if data_format='channels_first'
   *
   * or 4D tensor with shape:
   * `(samples, rows, cols, channels)` if data_format='channels_last'.
   *
   * Output shape
   *
   * 4D tensor with shape:
   * `(samples, filters, new_rows, new_cols)` if data_format='channels_first'
   *
   * or 4D tensor with shape:
   * `(samples, new_rows, new_cols, filters)` if data_format='channels_last'.
   * `rows` and `cols` values might have changed due to padding.
   */
  def config2Conv2D_V2(layer:Config): keras2.layers.Conv2D[Float] = {

    val params = layer.getConfig("params")
    /*
     * The dimensionality of the output space, i.e. the number of
     * convolution filters to use.
     */
    val filters:Int = params.getInt("filters")
    /*
     * An integer or tuple/list of a single integer, specifying the
     * length of the 1D convolution window.
     */
    val kernelSize = try {
      getAsIntArray(params.getList("kernelSize"))

    } catch {
      case t:Throwable => Array.empty[Int]
    }
    /*
     * An integer or tuple/list of a single integer, specifying the
     * stride length of the convolution.
     *
     * Specifying any stride value != 1 is incompatible with specifying
     * any `dilation_rate` value != 1.
     */
    val strides = try {
      getAsIntArray(params.getList("strides"))

    } catch {
      case t:Throwable => Array(1, 1)
    }

    /*
     * Either 'valid', 'causal' or 'same'. Default is 'valid'.
     *
     * `valid` means "no padding".
     *
     * `same` results in padding the input such that the output
     * has the same length as the original input
     *
     * `causal` results in causal (dilated) convolutions, e.g.
     * output[t] does not depend on input[t+1:]. Useful when
     * modeling temporal data where the model should not violate
     * the temporal order.
     */
    val padding = getAsString(params, "padding", "valid")
    /*
     * One of `channels_last` (default) or `channels_first`.
     *
     * The ordering of the dimensions in the inputs.
     *
     * `channels_last` corresponds to inputs with shape
     *
     * `(batch, height, width, channels)` while
     *
     * `channels_first` corresponds to inputs with shape
     * `(batch, channels, height, width)`.
     */
    val dataFormat = getAsString(params, "dataFormat", "channels_first")

    /*
     * Activation function to use. Default is null. You can also
     * pass in corresponding string representations such as 'relu'
     * or 'sigmoid', etc. for simple activations in the factory method.
     */
    val activation:String = try {
      params.getString("activation")

    } catch {
      case t:Throwable => null
    }
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, "bias", default = true)
    /*
     * Initializer for the `kernel` weights matrix.
     */
    val kernelInitializer = getAsString(params, "kernelInitializer", "glorot_uniform")
    /*
     * Initializer for the bias vector.
     */
    val biasInitializer = getAsString(params,"biasInitializer", "zero")
    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     *
     * NOTE: This is the kernel regularizer in Keras2
     */
    val kernelRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val biasRegularizer = params2RegularizerB(params)

    keras2.layers.Conv2D(
      filters = filters,
      kernelSize = kernelSize,
      strides = strides,
      padding = padding,
      dataFormat = dataFormat,
      activation = activation,
      useBias = bias,
      kernelInitializer = kernelInitializer,
      biasInitializer = biasInitializer,
      kernelRegularizer = kernelRegularizer,
      biasRegularizer = biasRegularizer)

  }

  /**
   * Convolutional LSTM.
   *
   * Note that currently only 'same' padding is supported.
   *
   * The convolution kernel for this layer is a square kernel with
   * equal strides.
   *
   * The input of this layer should be 5D, i.e. (samples, time, channels,
   * rows, cols), and 'CHANNEL_FIRST' (dimOrdering='th') is expected.
   *
   */
  def config2ConvLSTM2D(layer:Config): keras.layers.ConvLSTM2D[Float] = {

    val params = layer.getConfig("params")

    /* Activation function to use. You can also pass in corresponding
     * string representations such as 'relu' or 'sigmoid', etc. for
     * simple activations in the factory method. Default is 'tanh'.
     */
    val activation:String = getAsString(params, "activation", "tanh")
    /*
     * Activation function for inner cells. You can also pass in corresponding
     * string representations such as 'relu' or 'sigmoid', etc. for simple
     * activations in the factory method. Default is 'hard_sigmoid'.
     */
    val innerActivation:String = getAsString(params, "innerActivation", "hard_sigmoid")

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val borderMode = getAsString(params, "borderMode", "valid")

    /* Format of input data. Please use "CHANNEL_FIRST" (dimOrdering='th'). */
    val dimOrdering = getAsString(params, "dimOrdering", "th")

    /*
     * Whether the input sequence will be processed backwards.
     * Default is false.
     */
    val goBackwards = getAsBoolean(params, "goBackwards", default = false)

    /* Number of convolution filters to use. */
    val nbFilter:Int = params.getInt("nbFilter")

    /* Number of rows/columns in the convolution kernel. Square kernel. */
    val nbKernel:Int = params.getInt("nbKernel")

    /*
     * Whether to return the full sequence or only return
     * the last output in the output sequence. Default is false.
     */
    val returnSequences = getAsBoolean(params, "returnSequences", default = false)

    /*
     * Factor by which to subsample output. Also called strides
     * elsewhere. Default is 1.
     */
    val subsample = getAsInt(params, "subsample", 1)
    /*
     * Bias regularizer
     *
     *  An instance of [[Regularizer]], applied to the bias.
     *  Default is null.
     */
    val bRegularizer = params2RegularizerB(params)
    /*
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the recurrent weights matrices. Default is null.
     */
    val uRegularizer = params2RegularizerU(params)

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val wRegularizer = params2RegularizerW(params)

    keras.layers.ConvLSTM2D(
      activation = activation,
      borderMode = borderMode,
      dimOrdering = dimOrdering,
      goBackwards = goBackwards,
      innerActivation = innerActivation,
      nbFilter = nbFilter,
      nbKernel = nbKernel,
      returnSequences = returnSequences,
      subsample = subsample,
      wRegularizer = wRegularizer,
      bRegularizer = bRegularizer,
      uRegularizer = uRegularizer)

  }

  def config2Dense_V1(layer:Config): keras.layers.Dense[Float] = {

    val params = layer.getConfig("params")

    /* The size of output dimension. */
    val outputDim:Int = params.getInt("outputDim")

    /* Activation function to use. */
    val activation:String = getAsString(params, "activation", null)

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val wRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = params2RegularizerB(params)
    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, "bias", default = true)

    keras.layers.Dense(
      outputDim = outputDim,
      activation = activation,
      wRegularizer = wRegularizer,
      bRegularizer = bRegularizer,
      bias = bias)

  }
  /*
   * A densely-connected NN layer. The most common input is 2D.
   */
  def config2Dense_V2(layer:Config): keras2.layers.Dense[Float] = {

    val params = layer.getConfig("params")

    /* The size of output dimension. */
    val units:Int = params.getInt("units")

    /* Activation function to use. */
    val activation:String = getAsString(params, "activation", null)
    /*
     * Initializer for the `kernel` weights matrix. Default is Xavier.
     * You can also pass in corresponding string representations such
     * as 'glorot_uniform' or 'normal', etc. for simple init methods
     * in the factory method:
     *
     * - glorot_uniform
     * - one
     * - zero
     * - uniform
     * - normal
     */
    val kernelInitializer = getAsString(params, "kernelInitializer", "glorot_uniform")
    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val kernelRegularizer = params2RegularizerW(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val biasRegularizer = params2RegularizerB(params)

    /*
     * Whether to include a bias (i.e. make the layer affine rather
     * than linear). Default is true.
     */
    val bias = getAsBoolean(params, "bias", default = true)

    keras2.layers.Dense(
      units = units,
      activation = activation,
      kernelInitializer = kernelInitializer,
      kernelRegularizer = kernelRegularizer,
      biasRegularizer = biasRegularizer,
      useBias = bias
    )

  }

  def config2Dropout_V1(layer:Config): keras.layers.Dropout[Float] = {

    val params = layer.getConfig("params")

    /* Fraction of the input units to drop. Double between 0 and 1. */
    val p:Double = params.getDouble("p")

    keras.layers.Dropout(p = p)

  }

  /**
   * Applies Dropout to the input by randomly setting a fraction 'rate'
   * of input units to 0 at each update during training time in order
   * to prevent overfitting.
   */
  def config2Dropout_V2(layer:Config): keras2.layers.Dropout[Float] = {

    val params = layer.getConfig("params")

    /* Fraction of the input units to drop. Double between 0 and 1. */
    val rate:Double = params.getDouble("rate")

    keras2.layers.Dropout(rate = rate)

  }

  def config2ELU(layer:Config): ELU[Float] = {

    val params = layer.getConfig("params")

    /* Scale for the negative factor. Default is 1.0. */

    val alpha = getAsDouble(params, "alpha", 1.0)
    keras.layers.ELU(alpha = alpha)

  }
  /*
   * Flatten the results to feed into a dense layer
   */
  def config2Flatten_V1(layer:Config): Flatten[Float] = {
    keras.layers.Flatten()
  }

  def config2Flatten_V2(layer:Config): layers.Flatten[Float] = {
    keras2.layers.Flatten()
  }

  def config2GlobalAvgPool1D_V1(layer:Config): keras.layers.GlobalAveragePooling1D[Float] = {
    keras.layers.GlobalAveragePooling1D()
  }

  def config2GlobalAvgPool1D_V2(layer:Config): keras2.layers.GlobalAveragePooling1D[Float] = {
    keras2.layers.GlobalAveragePooling1D()
  }

  def config2GlobalMaxPool1D_V1(layer:Config): keras.layers.GlobalMaxPooling1D[Float] = {
    keras.layers.GlobalMaxPooling1D()
  }

  def config2GlobalMaxPool1D_V2(layer:Config): keras2.layers.GlobalMaxPooling1D[Float] = {
    keras2.layers.GlobalMaxPooling1D()
  }

  /* Gated Recurrent Unit architecture. */
  def config2GRU(layer:Config): keras.layers.GRU[Float] = {

    val params = layer.getConfig("params")

    /*
     * Hidden unit size. Dimension of internal projections and
     * final output.
     */
    val outputDim = params.getInt("outputDim")
    /*
     * Activation function to use. Default is 'tanh'.
     */
    val activation = getAsString(params, "activation", "tanh")
    /*
     * Activation function for inner cells. Default is 'hard_sigmoid'.
     */
    val innerActivation = getAsString(params, "innerActivation", "hard_sigmoid")
    /*
      * Whether to return the full sequence or only return
      * the last output in the output sequence. Default is false.
      */
    val returnSequences = getAsBoolean(params, "returnSequences", default = false)
    /*
     * Whether the input sequence will be processed backwards. Default is false.
     */
    val goBackwards = getAsBoolean(params, "goBackwards", default = false)

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     */
    val wRegularizer = params2RegularizerW(params)
    /*
     * An instance of [[Regularizer]], applied the recurrent weights
     * matrices. Default is null.
     */
    val uRegularizer = params2RegularizerU(params)
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = params2RegularizerB(params)

    keras.layers.GRU(
      outputDim = outputDim,
      activation = activation,
      innerActivation = innerActivation,
      returnSequences = returnSequences,
      goBackwards = goBackwards,
      wRegularizer = wRegularizer,
      uRegularizer = uRegularizer,
      bRegularizer = bRegularizer)

  }

  def config2LSTM(layer:Config): keras.layers.LSTM[Float] = {

    val params = layer.getConfig("params")

    /*
     * Hidden unit size. Dimension of internal projections
     * and final output.
     */
    val outputDim = params.getInt("outputDim")

    /* Activation function to use. */
    val activation = getAsString(params, "activation", "tanh")

    /* Activation function for inner cells. */
    val innerActivation = getAsString(params, "innerActivation", "hard_sigmoid")

    /*
     * Whether to return the full sequence or only return
     * the last output in the output sequence. Default is false.
     */
    val returnSequences = getAsBoolean(params, "returnSequences", default = false)

    /*
     * Whether the input sequence will be processed backwards.
     * Default is false.
     */
    val goBackwards = getAsBoolean(params, "goBackwards", default = false)

    keras.layers.LSTM(
      outputDim = outputDim,
      activation = activation,
      innerActivation = innerActivation,
      returnSequences = returnSequences,
      goBackwards = goBackwards)

  }

  def config2MaxPooling1D_V1(layer:Config): keras.layers.MaxPooling1D[Float] = {

    val params = layer.getConfig("params")

    /*
     * Size of the region to which max pooling is applied.
     * Integer. Default is 2.
     */
    val poolLength = getAsInt(params, "poolLength", 2)
    /*
     * Factor by which to downscale. Integer, or -1. 2 will
     * halve the input. If -1, it will default to poolLength.
     * Default is -1.
     */
    val stride = getAsInt(params, "stride", -1)

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val borderMode = getAsString(params, "borderMode", "valid")

    keras.layers.MaxPooling1D(
      poolLength = poolLength,
      stride = stride,
      borderMode = borderMode)

  }

  def config2MaxPooling1D_V2(layer:Config): keras2.layers.MaxPooling1D[Float] = {

    val params = layer.getConfig("params")

    /*
     * Size of the region to which max pooling is applied.
     * Integer. Default is 2.
     */
    val poolSize = getAsInt(params, "poolSize", 2)
    /*
     * Factor by which to downscale. Integer, or -1. 2 will
     * halve the input. If -1, it will default to poolLength.
     * Default is -1.
     */
    val poolStrides = getAsInt(params, "poolStrides", -1)

    /* Either 'valid' or 'same'. Default is 'valid'. */
    val padding = getAsString(params, "padding", "valid")

    keras2.layers.MaxPooling1D(
      poolSize = poolSize,
      strides = poolStrides,
      padding = padding)

  }

  def config2MaxPooling2D(layer:Config): keras.layers.MaxPooling2D[Float] = {

    val params = layer.getConfig("params")
    /*
     * Int tuple of length 2 corresponding to the downscale
     * vertically and horizontally. Default is (2, 2), which
     * will halve the image in each dimension.
     */
    val poolSize = getAsIntArray(params.getList("poolSize"))
    keras.layers.MaxPooling2D(poolSize = (poolSize(0), poolSize(1)))

  }

  /*
   * Repeats the input n times. The input of this layer should be 2D.
   */

  def config2RepeatVector(layer:Config): keras.layers.RepeatVector[Float] = {

    val params = layer.getConfig("params")
    /*
     * Repetition factor. Integer.
     */
    val n = params.getInt("n")
    keras.layers.RepeatVector(n = n)

  }

  def config2Shape(input:ConfigList):Shape = {

    try {

      val dimensions = getAsIntArray(input)
      val shape = Shape(dimensions: _*)
      shape

    } catch {
      case t:Throwable => throw new Exception("Input shape cannot be built.")
    }

  }

  def params2RegularizerB(params:Config): L1L2Regularizer[Float] = {
    /*
     * Bias regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the bias. Default is null.
     */
    val bRegularizer = try {

      val bReg = params.getConfig("bRegularizer")
      config2Regularizer(bReg)

    } catch {
      case t:Throwable => null
    }

    bRegularizer

  }

  def params2RegularizerU(params:Config): L1L2Regularizer[Float] = {
    /*
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
 		 * applied to the recurrent weights matrices. Default is null.
     */
    val uRegularizer = try {

      val uReg = params.getConfig("uRegularizer")
      config2Regularizer(uReg)

    } catch {
      case t:Throwable => null
    }

    uRegularizer

  }

  def params2RegularizerW(params:Config): L1L2Regularizer[Float] = {

    /*
     * Weight regularizer
     *
     * An instance of [[Regularizer]], (eg. L1 or L2 regularization),
     * applied to the input weights matrices. Default is null.
     *
     * NOTE: This is the kernel regularizer in Keras2
     */
    val wRegularizer = try {

      val wReg = params.getConfig("wRegularizer")
      config2Regularizer(wReg)

    } catch {
      case t:Throwable => null
    }

    wRegularizer

  }

}

