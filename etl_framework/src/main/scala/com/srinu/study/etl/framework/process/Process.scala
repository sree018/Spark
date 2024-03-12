package com.srinu.study.etl.framework.process

import com.srinu.spark.etl.farmework.file.ingestion.Ingestion
import com.srinu.study.etl.framework.drools.DroolsRules
import com.srinu.study.etl.framework.logging.Logging
import com.srinu.study.etl.framework.sas.SasUtils
import com.srinu.study.etl.framework.utils.EtlUtils
import com.typesafe.config._
import org.apache.spark.sql.SparkSession

/**
 *
 */

class Process(args: Array[String]) extends Logging {
  try {
    val processType: String = new EtlUtils().isEmptyArg(args(1).trim.toUpperCase)
    val config: Config = ConfigFactory.parseFile(new java.io.File(args(0)))
    val appName: String = config.getString("appName")
    logger.info(s"$processType Job started for $appName")
    val spark: SparkSession = SparkSession.builder.appName(appName).master("local[*]").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info(s"spark appId : ${spark.sparkContext.applicationId}")
    val business_date: String = if (args.indices.contains(2)) {
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
        new Ingestion(spark, config, filePath, business_date).ingestionOfInputSources()
      }
      case "TRANSFORMATION" => //new EtlTransformation(spark, config, business_date)
      case "EXTRACT" => //new EtlFileWriter(spark, config, business_date).fileWriter()
      case "DROOLS" =>
        val drlFilePath: String = if (args.indices.contains(3)) {
          args(3)
        } else {
          throw new Exception(s"args(3) is raw drlFilePath, please check args for $processType")
        }
        new DroolsRules(spark, config, business_date, drlFilePath).applyDrools()
      case "SAS" => {
        val sasFilePath: String = if (args.indices.contains(3)) {
          args(3)
        } else {
          throw new Exception(s"args(3) is raw filePath, please check args for $processType")
        }

        new SasUtils().sasDetails(spark, config, business_date, sasFilePath)
      }
      case _ => throw new Exception("args(1) is type of etl like INGESTION,TRANSFORMATION,DROOLS,SAS and EXTRACT")
    }
  } catch {
    case ex: Exception => {
      logger.info(ex.getMessage)
      logger.info(ex.getStackTrace.mkString("\n"))
      System.exit(1)
    }
  }

}