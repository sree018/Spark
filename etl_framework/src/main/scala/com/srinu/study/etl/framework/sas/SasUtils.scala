package com.srinu.study.etl.framework.sas

import com.srinu.study.etl.framework.logging.Logging
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class SasUtils extends Logging with Serializable {
   def sasDetails(spark:SparkSession, config:Config, business_date:String, filePath:String)={
     new SasReader().readSasData(spark,filePath)
   }
}
