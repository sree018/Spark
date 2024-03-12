package com.srinu.study.etl.framework.utils

import com.srinu.study.etl.framework.logging.Logging
import com.typesafe.config.Config
import org.apache.hadoop.fs._
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
 * @author sree018
 */

class EtlUtils extends Logging with Serializable {
  def isEmptyArg(inputArg: String): String = {
    if (inputArg.nonEmpty) {
      inputArg
    } else {
      throw new Exception(s"InvalidParameter : '$inputArg' ")
    }
  }

  def configStatus(config: Config, configParam: String): String = {
    if (config.hasPath(configParam)) {
      config.getString(configParam)
    } else {
      throw new Exception(s"$configParam is not exist in $config")
    }
  }


  def mergeSmallFilesInHive(spark: SparkSession,
                            partitionValues:Map[String,String],
                            partitionColumns: Seq[String],
                            schemaName: String,
                            tableName: String): Unit = {
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val partitionData: DataFrame = spark.table(s"$schemaName.$tableName")
      .where(partitionValues.map { case (col, value) => s"$col = '$value'" }.mkString(" AND "))
    val maxSizeThreshold: Long = 256 * 1024 * 1024
    val totalSize: Long = partitionData.selectExpr("sum(size) as total_size").head().getLong(0)
    if (totalSize < maxSizeThreshold) {
      partitionData.repartition(1).write.mode("overwrite")
        .partitionBy(partitionColumns: _*)
        .format("parquet").insertInto(s"${partitionData.sparkSession.catalog.currentDatabase}.$tableName")
    }
  }
}
