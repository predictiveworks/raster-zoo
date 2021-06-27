package com.intel.analytics.zoo.common
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
import org.apache.spark.{SPARK_VERSION, SparkConf, SparkException}
import com.intel.analytics.bigdl.utils.Engine

object ZooHelper {

  def initEngine:Unit = {
    Engine.init
  }

  def initConf(sparkConf: SparkConf): SparkConf = {
   /**
    * Leverage the configuration to generate an
    * Analytics Zoo compliant Apache Spark Conf
    */
    val zooConf = NNContext.createSparkConf(sparkConf)
    /**
     * Check env and set spark conf, and set default value.
     * We should skip this env, when engineType is mkldnn.
     */
    if (System.getProperty("bigdl.engineType", "mklblas")
      .toLowerCase() == "mklblas") {

      val kmpAffinity = sys.env.getOrElse("KMP_AFFINITY", "granularity=fine,compact,1,0")
      val kmpBlockTime = sys.env.getOrElse("KMP_BLOCKTIME", "0")
      val kmpSettings = sys.env.getOrElse("KMP_SETTINGS", "1")
      val ompNumThreads = if (sys.env.contains("ZOO_NUM_MKLTHREADS")) {
        if (sys.env("ZOO_NUM_MKLTHREADS").equalsIgnoreCase("all")) {
          zooConf.get("spark.executor.cores", Runtime.getRuntime.availableProcessors().toString)
        } else {
          sys.env("ZOO_NUM_MKLTHREADS")
        }
      } else if (sys.env.contains("OMP_NUM_THREADS")) {
        sys.env("OMP_NUM_THREADS")
      } else {
        "1"
      }

      /* Set Spark Conf */
      zooConf.setExecutorEnv("KMP_AFFINITY",    kmpAffinity)
      zooConf.setExecutorEnv("KMP_BLOCKTIME",   kmpBlockTime)
      zooConf.setExecutorEnv("KMP_SETTINGS",    kmpSettings)
      zooConf.setExecutorEnv("OMP_NUM_THREADS", ompNumThreads)

    }

    if (zooConf.getBoolean("spark.analytics.zoo.versionCheck", defaultValue = false)) {
      val reportWarning =
        zooConf.getBoolean("spark.analytics.zoo.versionCheck.warning", defaultValue = false)

      checkSparkVersion(reportWarning)
      checkScalaVersion(reportWarning)

    }

    zooConf

  }

  /** HELPER METHODS **/

  private def checkScalaVersion(reportWarning: Boolean = false) = {
    checkVersion(scala.util.Properties.versionNumberString,
      ZooBuildInfo.scala_version, "Scala", reportWarning, level = 2)
  }

  private def checkSparkVersion(reportWarning: Boolean = false) = {
    checkVersion(SPARK_VERSION, ZooBuildInfo.spark_version, "Spark", reportWarning)
  }

  private def checkVersion(
                            runtimeVersion: String,
                            compileTimeVersion: String,
                            project: String,
                            reportWarning: Boolean = false,
                            level: Int = 1): Unit = {
    val Array(runtimeMajor, runtimeFeature, runtimeMaintenance) =
      runtimeVersion.split("\\.").map(_.toInt)
    val Array(compileMajor, compileFeature, compileMaintenance) =
      compileTimeVersion.split("\\.").map(_.toInt)

    if (runtimeVersion != compileTimeVersion) {
      val warnMessage = s"The compile time $project version is not compatible with" +
        s" the runtime $project version. Compile time version is $compileTimeVersion," +
        s" runtime version is $runtimeVersion. "
      val errorMessage = s"\nIf you want to bypass this check, please set" +
        s"spark.analytics.zoo.versionCheck to false, and if you want to only" +
        s"report a warning message, please set spark.analytics.zoo" +
        s".versionCheck.warning to true."
      val diffLevel = if (runtimeMajor != compileMajor) {
        1
      } else if (runtimeFeature != compileFeature) {
        2
      } else {
        3
      }
      if (diffLevel <= level && !reportWarning) {
        Utils.logUsageErrorAndThrowException(warnMessage + errorMessage)
      }
      println(warnMessage)

    } else {
      println(s"$project version check pass")
    }
  }

}

object ZooBuildInfo {

  val (
    analytics_zoo_version: String,
    spark_version: String,
    scala_version: String,
    java_version: String) = {

    val resourceStream = Thread.currentThread().getContextClassLoader.
      getResourceAsStream("zoo-version-info.properties")

    try {
      val unknownProp = "<unknown>"
      val props = new java.util.Properties()
      props.load(resourceStream)
      (
        props.getProperty("analytics_zoo_version", unknownProp),
        props.getProperty("spark_version", unknownProp),
        props.getProperty("scala_version", unknownProp),
        props.getProperty("java_version", unknownProp)
      )
    } catch {
      case npe: NullPointerException =>
        throw new RuntimeException("Error while locating file zoo-version-info.properties, " +
          "if you are using an IDE to run your program, please make sure the mvn" +
          " generate-resources phase is executed and a zoo-version-info.properties file" +
          " is located in zoo/target/extra-resources", npe)
      case e: Exception =>
        throw new RuntimeException("Error loading properties from zoo-version-info.properties", e)
    } finally {
      if (resourceStream != null) {
        try {
          resourceStream.close()
        } catch {
          case e: Exception =>
            throw new SparkException("Error closing zoo build info resource stream", e)
        }
      }
    }
  }
}
