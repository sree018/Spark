package com.srinu.spark.etl.farmework.file.sources

import com.srinu.spark.etl.farmework.logging.Logging
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class AvroFiles (spark:SparkSession,config:Config,filePath:String) extends Logging with Serializable {
  def parseInputFile():DataFrame={
    spark.emptyDataFrame
  }
}