package com.srinu.spark.etl.farmework.logging

import org.apache.log4j._

/**
 * @author sree dama
 */
trait Logging {
  val logger: Logger = Logger.getLogger(getClass.getName)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
}