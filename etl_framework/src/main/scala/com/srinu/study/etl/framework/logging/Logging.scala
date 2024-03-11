package com.srinu.study.etl.framework.logging

import org.apache.log4j._

/**
 * @author sree dama
 */
trait Logging {
  val logger: Logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)
}
