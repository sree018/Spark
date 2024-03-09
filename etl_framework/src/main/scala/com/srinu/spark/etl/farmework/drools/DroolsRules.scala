package com.srinu.spark.etl.farmework.drools

import com.srinu.spark.etl.farmework.logging.Logging
import com.typesafe.config.Config
import org.apache.spark.sql._

class DroolsRules(spark: SparkSession, config: Config, inputDate: String, drlFilePath: String) extends Logging with Serializable {
  val excelSheetName: String = "salesOrder"
  val drlFile: String = new ReadExcelRules().getExcelData(spark, drlFilePath, excelSheetName)
  val path:String = "C:\\Users\\sdama\\OneDrive\\Desktop\\Financial_Sample.csv"
  val df:DataFrame = spark.read.format("csv").options(Map("delimiter" -> "|", "header" -> "true")).load(path)
  df.rdd.mapPartitions(partition=>{
    new KieSessionUtils().GetKnowledgeSession()
    partition.map(row=>{
      println(row)
    })
  }).count()
}