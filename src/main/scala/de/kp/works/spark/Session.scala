package de.kp.works.spark
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
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import com.intel.analytics.zoo.common.ZooHelper
/**
 * These `implicits` are important as they provide
 * additional functionality for `SparkSession`
 */
import org.locationtech.rasterframes.extensions.Implicits._

object Session {

  private var session:Option[SparkSession] = None

  def initialize():Unit = {

    println("[INFO]")
    println("[INFO] Spark Session initialization")
    println("[INFO]")

    val conf = new SparkConf()
      .setAppName("RasterZoo")
      .setMaster("local[4]")
      /*
       * Driver & executor configuration
       */
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.driver.memory"       , "12g")
      .set("spark.executor.memory"     , "12g")
      /**
       * BigDL specific configuration, extracted from spark-bigdl.conf  :
       * ----------------------------------------------------------------
       * spark.shuffle.reduceLocality.enabled             false
       * spark.shuffle.blockTransferService               nio
       * spark.scheduler.minRegisteredResourcesRatio      1.0
       * spark.speculation                                false
       */
      .set("spark.shuffle.reduceLocality.enabled",        "false")
      .set("spark.shuffle.blockTransferService",          "nio")
      .set("spark.scheduler.minRegisteredResourcesRatio", "1.0")
      .set("spark.speculation",                           "false")

    /**
     * This is the bridge between Apache Spark and
     * Analytics Zoo
     */
    val zooConf = ZooHelper.initConf(conf)

    /**
     * This is the bridge between Apache Spark and RasterFrames.
     * Important is `Kryo` serialization and the `withRasterFrames`
     * extension of the plain SparkSession
     */
    val spark = SparkSession.builder()
      .config(zooConf)
      .withKryoSerialization
      .getOrCreate()
      .withRasterFrames

    /**
     * This is the bridge between Apache Spark and
     * Analytics Zoo
     */
    ZooHelper.initEngine
    session = Option(spark)

  }

  def getSession:SparkSession = {

    if (session.isEmpty) initialize()
    session.get

  }
}
