package com.srinu.study.etl.framework.utils

import com.srinu.study.etl.framework.logging.Logging

import com.typesafe.config.Config

/**
 * @author sree018
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
