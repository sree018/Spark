package com.srinu.spark.etl.farmework.process

import com.srinu.spark.etl.farmework.drools.DroolsRules
import com.srinu.spark.etl.farmework.file.extract.EtlFileWriter
import com.srinu.spark.etl.farmework.file.ingestion.Ingestion
import com.srinu.spark.etl.farmework.logging.Logging
import com.srinu.spark.etl.farmework.transformation.EtlTransformation
import com.srinu.spark.etl.farmework.utils.EtlUtils
import com.typesafe.config._
import org.apache.spark.sql.SparkSession

/**
 *
 */

class Process(args: Array[String]) extends Logging with Serializable {
  try {
    val processType: String = new EtlUtils().isEmptyArg(args(1).trim.toUpperCase)
    val config: Config = ConfigFactory.parseFile(new java.io.File(args(0)))
    val appName: String = config.getString("appName")
    logger.info(s"$processType Job started for $appName")
    val spark: SparkSession = SparkSession.builder.appName(appName).master("local[*]").getOrCreate()
    logger.info(s"spark appId : ${spark.sparkContext.applicationId}")
    spark.sparkContext.setLogLevel("FATAL")
    val bussiness_date: String = if (args.indices.contains(2)) {
      if (args(2).trim.matches("\\d+")) args(2) else throw new Exception(s"args(2) is business_date, should be numberic")
    } else {
      throw new Exception(s"args(2) is business_date, please check args for $processType")
    }
    processType match {
      case "INGESTION" => {
        val filePath: String = if (args.indices.contains(3)) {
          args(3)
        } else {
          throw new Exception(s"args(3) is raw filePath, please check args for $processType")
        }
        new Ingestion(spark, config, filePath, bussiness_date).ingestionOfInputSources()
      }
      case "TRANSFORMATION" => new EtlTransformation(spark, config, bussiness_date)
      case "EXTRACT"        => new EtlFileWriter(spark, config, bussiness_date).fileWriter()
      case "DROOLS"         =>
        val drlFilePath: String = if (args.indices.contains(3)) {
          args(3)
        } else {
          throw new Exception(s"args(3) is raw drlFilePath, please check args for $processType")
        }
        new DroolsRules(spark, config, bussiness_date,drlFilePath)
      case _                => throw new Exception("args(1) is type of etl like INGESTION,TRANSFORMATION,DROOLS and EXTRACT")
    }
  } catch {
    case ex: Exception => {
      logger.info(ex.getMessage)
      logger.info(ex.getStackTrace.mkString("\n"))
      System.exit(1)
    }
  }

}