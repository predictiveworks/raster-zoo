package de.kp.works.model.build
/**
 * Copyright (c) 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import com.typesafe.config.{Config,ConfigFactory}

object DLConfig {

  private val path = "works-dl.conf"
  /*
   * This is the reference to the overall configuration
   * file that holds all configuration required for this
   * application
   */
  private val cfg:Config = init

  private def init:Config = {

    try {
      ConfigFactory.load(path)

    } catch {
      case _:Throwable => null
    }

  }

  def getFolder:String = {

    val modelCfg = getModelCfg
    modelCfg.getString("folder")

  }

  def getModelCfg:Config = {

    if (cfg == null)
      throw new Exception("Deep learning module is not configured yet.")

    cfg.getConfig("model")

  }

}
