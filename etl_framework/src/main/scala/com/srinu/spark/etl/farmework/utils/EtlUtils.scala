package com.srinu.spark.etl.farmework.utils

import com.srinu.spark.etl.farmework.logging.Logging
import com.typesafe.config.Config

/**
 * @author srini dama
 */

class EtlUtils extends Logging with Serializable {
  def isEmptyArg(inputArg: String): String = {
    if (inputArg.nonEmpty) {
      inputArg
    } else {
      throw new Exception(s"InvalidParameter : '$inputArg' ")
    }
  }

  def configStatus(config: Config, configParam: String): String = {
    if (config.hasPath(configParam)) {
      config.getString(configParam)
    } else {
      throw new Exception(s"$configParam is not exist in $config")
    }
  }
}
