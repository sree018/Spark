package com.srinu.spark.etl.farmework.sources

import com.srinu.spark.etl.farmework.fixedWidthUtils.FixedWidthUtils
import com.srinu.spark.etl.farmework.logging.Logging
import com.srinu.spark.etl.farmework.utils.EtlUtils
import com.typesafe.config.Config
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StringType}

class FixedWidthFiles(spark: SparkSession, config: Config, filePath: String) extends Logging with Serializable {
  def parseInputFile(): DataFrame = {
    val inputCodec: String = config.getString("charSet").trim
    val charSet: String = if (inputCodec.isEmpty) "ISO-8859-1" else inputCodec
    val fixedWidthConfig: Config = config.getConfig("fixedWidthOptions")
    logger.info(s"input file reading with $charSet charset")
    val rawFileRdd: RDD[String] = spark.sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](filePath)
      .map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charSet))
    val layout: String = new EtlUtils().configStatus(fixedWidthConfig, "layOutFileName")
    val layoutPath: String = "C:\\Users\\sdama\\Downloads"
    val layOutSchema: Seq[(String, Int, DataType)] = new FixedWidthUtils().getLayoutDetails(spark, layoutPath, layout)
    val parsedDf: DataFrame = new FixedWidthUtils().parseRddFile(rawFileRdd, layOutSchema)(spark)
    parsedDf
  }
}
