package com.srinu.spark.etl.farmework.file.ingestion.fixedwidth.utils

import com.srinu.spark.etl.farmework.logging.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class FixedWidthUtils extends Logging with Serializable {
  def getLayoutDetails(spark: SparkSession, layoutPath: String, layoutFileName: String): Seq[(String, Int, DataType)] = {
    val filePath: String = s"$layoutPath/$layoutFileName"
    val layoutRdd: Array[String] = spark.sparkContext.textFile(filePath, 1).repartition(1).collect()
    val offSetDetails: Seq[(String, Int, DataType)] = layoutRdd.map(line => {
      val schemaDetails: Array[String] = line.split(",")
      val colName: String = schemaDetails.head
      val colLength: String = schemaDetails.last
      val dataType: String = colLength.substring(0, 1)
      val lengthOfString: String = colLength.substring(1, 5)
      if (!lengthOfString.matches("\\d+")) {
        throw new Exception(s"length of column invalid for $colName,'$dataType$lengthOfString'")
      }
      dataType match {
        case "S" =>
          val headTwoDigits: Int = lengthOfString.substring(0, 2).toInt
          val tailTwoDigits: Int = lengthOfString.substring(2, 4).toInt
          if (tailTwoDigits == 0) {
            (colName, headTwoDigits, if (headTwoDigits <= 9) IntegerType else LongType)
          } else {
            if (headTwoDigits > tailTwoDigits) {
              (colName, headTwoDigits + tailTwoDigits + 1, DecimalType(headTwoDigits, tailTwoDigits))
            } else {
              (colName, headTwoDigits + tailTwoDigits + 1, DecimalType(headTwoDigits + tailTwoDigits, tailTwoDigits))
            }
          }
        case "T" => (colName, lengthOfString.trim.toInt, StringType)
        case _ => throw new Exception("please use proper data type start with 'S' -> ('bigint','int','decimal') or 'T' -> String")
      }
    })
    offSetDetails
  }

  def parseRddFile(rdd: RDD[String], recordLayOut: Seq[(String, Int, DataType)])(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val rawDf: DataFrame = rdd.toDF("row")
    var startOffSet: Int = 1
    val seqOfColumns:Seq[Column] = recordLayOut.map(line => {
      val colName: String = line._1
      val endOffSet: Int = line._2
      val dataType: DataType = line._3
      val subStrCol: Column = col("row").substr(startOffSet, endOffSet).as(colName)
      startOffSet += endOffSet
      subStrCol.cast(dataType)
    })
   val parseDf:DataFrame=rawDf.select(seqOfColumns:_*)
    parseDf
  }
}