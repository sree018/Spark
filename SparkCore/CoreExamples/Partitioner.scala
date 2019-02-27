package com.sparkPrepartions

import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner

object partitinoer {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder().appName("analysis of AirlinesData").master("local[*]").getOrCreate()
    val data = spark.sparkContext.textFile("/home/srinu/Desktop/data")
    val count = data.flatMap(line => line.split(" ")).map(word => (word, 1)).partitionBy(new HashPartitioner(5))
    count.reduceByKey(_+_).saveAsTextFile("/home/srinu/Desktop/hashdata2")
    val counts = data.flatMap(line => line.split(" ")).map(word => (word, 1))
      val rage = counts.partitionBy(new RangePartitioner(5,counts))
    rage.reduceByKey(_+_).saveAsTextFile("/home/srinu/Desktop/rangedata2")
  }
}
