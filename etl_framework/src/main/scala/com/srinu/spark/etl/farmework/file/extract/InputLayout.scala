package com.srinu.spark.etl.farmework.file.extract

import com.srinu.spark.etl.farmework.logging.Logging
import org.apache.spark.sql._
import com.srinu.spark.etl.farmework.utils.EtlUtils
import com.typesafe.config.Config
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.ScalaReflection
import com.srinu.spark.etl.farmework.utils.EtlUtils
import org.apache.spark.rdd._


class InputLayout(spark: SparkSession, config: Config) extends Logging with Serializable {
  val sparkPath: String = s"C:\\Users\\sdama\\workspace\\etl_framework\\src\\main\\resources"
  val layoutFileName: String = new EtlUtils().configStatus(config.getConfig("extractFileOptions"), "extractFileLayoutName")
  val layoutFile: String = s"$sparkPath/$layoutFileName"
  def getLayOutMaps(): Map[String, Map[String, String]] = {
    val mapSchema: List[(String, (DataType, Boolean))] = List(
      "src_col_name" -> (StringType, false), "trg_col_name" -> (StringType, false),
      "dataType" -> (StringType, false), "lengthOfColumn" -> (IntegerType, false),
      "scaleOfColumn" -> (IntegerType, true), "defaults" -> (StringType, true),
      "padValue" -> (StringType, true), "padDecision" -> (StringType, true),
      "expression" -> (StringType, true), "fieldNotes" -> (StringType, true))
    val schema: StructType = StructType(mapSchema.map(line => StructField(line._1, line._2._1, line._2._2)).toSeq)
    logger.info(s"input layout headers in csv file")
    schema.zipWithIndex.map(x => {
      logger.info(s"position : ${x._2},name : ${x._1.name}, dataType : ${x._1.dataType}, nullable : ${x._1.nullable}")
    })
    val layoutFields:Array[(Row,Int)]=spark.read.format("csv").option("header","true")
    .load(layoutFile).collect().zipWithIndex
  
    layoutFields.foreach(println)
   
    Map("" -> Map("" -> ""))
  }

}