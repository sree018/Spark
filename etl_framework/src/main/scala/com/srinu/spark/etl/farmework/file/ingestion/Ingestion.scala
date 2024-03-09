package com.srinu.spark.etl.farmework.file.ingestion

import com.srinu.spark.etl.farmework.file.sources.{AvroFiles, DelimitedFiles, EbcdicFiles, FixedWidthFiles, JsonFiles, ParquetFiles, XmlFiles}
import com.srinu.spark.etl.farmework.logging.Logging
import com.srinu.spark.etl.farmework.utils.EtlUtils
import com.typesafe.config.Config
import org.apache.spark.sql._

/**
 * @author srini dama
 * @param spark     :SparkSession
 * @param config    : config
 * @param filePath  : input file or dir
 * @param inputDate : business Date of files
 */
class Ingestion(spark: SparkSession, config: Config, filePath: String, inputDate: String) extends Logging {
  def ingestionOfInputSources(): Unit = {
    val ingestionTypes: List[String] = List("ebcdic", "fixedwidth", "delimited", "json", "xml", "parquet", "avro")
    val ingestionConfig:Config=config.getConfig("ingestionOptions")
    val ingestionType: String = new EtlUtils().configStatus(ingestionConfig, "ingestionType")
    logger.info(s"Ingesting $ingestionType file")
    val inputRawDf: DataFrame = ingestionType.trim.toUpperCase match {
      case "EBCDIC" => new EbcdicFiles(spark, ingestionConfig, filePath).parseInputFile()
      case "FIXEDWIDTH" => new FixedWidthFiles(spark, ingestionConfig, filePath).parseInputFile()
      case "DELIMITED" => new DelimitedFiles(spark, ingestionConfig, filePath).parseInputFile()
      case "JSON" => new JsonFiles(spark, ingestionConfig, filePath).parseInputFile()
      case "XML" => new XmlFiles(spark, ingestionConfig, filePath).parseInputFile()
      case "PARQUET" => new ParquetFiles(spark, ingestionConfig, filePath).parseInputFile()
      case "AVRO" => new AvroFiles(spark, ingestionConfig, filePath).parseInputFile()
      case _ => throw new Exception(s"ingestionType args like $ingestionTypes")
    }
    inputRawDf.show(false)
  }
}