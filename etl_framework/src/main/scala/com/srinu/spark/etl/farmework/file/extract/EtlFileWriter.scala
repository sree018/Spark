package com.srinu.spark.etl.farmework.file.extract

import com.srinu.spark.etl.farmework.logging.Logging
import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import com.srinu.spark.etl.farmework.utils.EtlUtils
import org.apache.spark.rdd._

/**
 * 
 */
class EtlFileWriter(spark: SparkSession, config: Config, inputDate: String) extends Logging {
 def fileWriter()={
   val layOutOfMaps=new InputLayout(spark, config).getLayOutMaps()
 }
  
}