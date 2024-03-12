package com.srinu.spark.etl.farmework.file.sources


import com.srinu.study.etl.framework.logging.Logging
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class EbcdicFiles (spark:SparkSession,config:Config,filePath:String) extends Logging with Serializable {
   def parseInputFile():DataFrame={
     spark.emptyDataFrame
   }
}