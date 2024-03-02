package com.srinu.spark.etl.farmework

import com.srinu.spark.etl.farmework.filewriter.EtlFileWriter
import com.srinu.spark.etl.farmework.ingestion.Ingestion
import com.srinu.spark.etl.farmework.logging.Logging
import com.srinu.spark.etl.farmework.transformation.EtlTransformation
import com.srinu.spark.etl.farmework.utils.EtlUtils
import com.typesafe.config._
import org.apache.spark.sql.SparkSession

import java.io.File

class Process(args: Array[String]) extends Logging with Serializable {
  try {
    val processType: String = new EtlUtils().isEmptyArg(args(1).trim.toUpperCase)
    val config: Config = ConfigFactory.parseFile(new File(args(0)))
    val appName: String = config.getString("appName")
    logger.info(s"$processType Job started for $appName")
    val spark: SparkSession = SparkSession.builder.appName(appName).master("local[*]").getOrCreate()
    logger.info(s"spark appId : ${spark.sparkContext.applicationId}")
    processType match {
      case "INGESTION" => new Ingestion(spark, config, args(3),args(2)).ingestionOfInputSources()
      case "TRANSFORMATION" => new EtlTransformation(spark , config, args(2))
      case "FILE_WRITE" => new EtlFileWriter(spark, config, args(2))
      case _ => throw new Exception("args(1) is type of etl like INGESTION,TRANSFORMATION and FILE_WRITE")
    }
  } catch {
    case ex: Exception => {
      logger.info(ex.getMessage)
      logger.info(ex.getStackTrace.mkString("\n"))
    }
  }

}
