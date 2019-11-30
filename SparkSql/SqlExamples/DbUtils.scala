package com.spark.rdbms.utils
import java.io.FileReader
import java.io.File
import java.util.Properties
import java.util.logging.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types.StructType

object DbUtils {
  val logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("============Pass the arguments to application=================")
      System.exit(1)
    }
    val fileArgs = args(0)
    val conn = new JdbcUtils()
    val param =conn.configFile(fileArgs)
    val appName = param.getProperty("source.app.appName")
    val appMaster = param.getProperty("source.app.appMaster")
    val hiveWareHouse = param.getProperty("source.hive.warehouse")
    val hiveSchema = param.getProperty("source.hive.schema").toBoolean
    logger.info("===appName===" + appName)
    logger.info("======" + appMaster)
    val spark = conn.sparkSession(appName, appMaster, hiveWareHouse, hiveSchema)
    val srcDbProps = conn.srDbUtils(param)
    val srcDbData = conn.loadSrcDbData(spark, param, srcDbProps)
//      
      
      
      
      //.write.jdbc(sinkDbConnProps.getProperty("url"), "indian_airport_data",sinkDbConnProps)
      
      //sourceDbData.write.mode("overwrite").insertInto("indian_airport_data")
     
//    val dbColums:Option[StructType] = Option(sourceDbData.schema)
//    
//    val dialect = JdbcDialects.get(sourcedbConnProps.getProperty("url"))
//    val keyColumns =List("Airport_ID","Name","City","Country")
    //val d = conn.getInsertStatement("indian_airport_data", sourceDbData, dbColums, true, dialect,keyColumns)
  }
}