package com.srinu.study.etl.framework.drools

import com.srinu.study.etl.framework.logging.Logging
import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.types.DoubleType
import org.kie.api.KieServices
import org.kie.api.runtime.KieContainer
import org.kie.api.runtime.StatelessKieSession
import org.apache.spark.sql.functions._

class DroolsRules(spark: SparkSession, config: Config, inputDate: String, drlFilePath: String) extends Logging
  with Serializable {
  def applyDrools(): Unit = {
    val excelSheetName: String = "salesOrder"
    val drlFile: String = new ReadExcelRules().getExcelData(spark, drlFilePath, excelSheetName)
    val path: String = "C:\\Users\\sdama\\OneDrive\\Desktop\\Financial_Sample.csv"
    val df: DataFrame = spark.read.format("csv").options(Map("delimiter" -> ",", "header" -> "true")).load(path)
    import spark.implicits._
    val transactionDS = df.withColumn("amount", col("amount1").cast(DoubleType))
      .select("amount", "Product").as[Transaction]
    val classifiedTransactions = transactionDS.mapPartitions(iter => {
      val classifier = new KieSessionUtils()
      iter.map(transaction => {
        classifier.classify(transaction)
        transaction
      })
    })
    classifiedTransactions.show()
  }


}