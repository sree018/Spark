package com.srinu.spark.etl.farmework.logging

import org.apache.log4j._


trait Logging {
   val logger: Logger = Logger.getLogger(getClass.getName)
}
