package com.jdbcAnlysis
import org.apache.spark.sql.SparkSession
object dataBaseToSpark {
  def main(args:Array[String]){
    val spark = SparkSession.builder().appName("studying Database to sparksql").master("local[*]").getOrCreate()
    val conn = new dataConnections()
    val credentials ="/home/srinu/Desktop/db.properties"
    val details = conn.dbConnections(credentials)
    //println(details.getProperty("url"))
    val d =spark.read.jdbc(details.getProperty("url"), "OrderTable", details)
    d.printSchema()
   
  }
}