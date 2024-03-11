package com.srinu.study.etl.framework.process

import com.srinu.study.etl.framework.logging._
import org.apache.hadoop.shaded.org.apache.commons.configuration2.SystemConfiguration


/**
 * @author Sree018
 */

object Runner extends Logging  {
  def main(args: Array[String]): Unit = {
    val startTime: Long = System.nanoTime()
    if (args.length < 3) {
      logger.error(s"####################: INPUT ARGUMENTS :####################")
      args.zipWithIndex.foreach(line => logger.error(s"args(${line._2}) : ${line._1}"))
      logger.error(s"Please submit enough arguments  to job like  config, process_type,business_date ")
      throw new IndexOutOfBoundsException(s"Please submit enough arguments  to job")
    } else {
      logger.info(s"####################: INPUT ARGUMENTS :####################")
      args.zipWithIndex.foreach(line => logger.info(s"args(${line._2}) : ${line._1}"))
      logger.info(s"###########################################################")
      new Process(args)
    }
    val endTime: Long = System.nanoTime()
    val totalTime: Long = ((endTime - startTime) / 1e9).toLong
    logger.info(s"Job completion time : $totalTime")
  }
}