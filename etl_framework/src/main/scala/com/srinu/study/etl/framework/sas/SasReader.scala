package com.srinu.study.etl.framework.sas

import com.srinu.study.etl.framework.logging.Logging
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.compress.CompressionCodecFactory
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

class SasReader extends Logging with Serializable {
  def readSasData(spark: SparkSession, filePath: String) = {
  }
}
