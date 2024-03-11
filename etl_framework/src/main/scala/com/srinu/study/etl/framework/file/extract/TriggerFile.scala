package com.srinu.study.etl.framework.file.extract

import com.srinu.study.etl.framework.logging.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._

class TriggerFile(implicit spark: SparkSession) extends Logging with Serializable {

  import spark.implicits._

  def inputData(extractCount: Long,
                seqOfMap: Map[String, String],
                colNotesMap: Map[String, String],
                dataTypeMap: Map[String, String],
                lengthMap: Map[String, String],
                defaultMap: Map[String, String]): DataFrame = {
    val sortOrder: Seq[(String, Int)] = seqOfMap.map(x => x._1 -> x._2.toInt).toSeq.sortWith(_._2 < _._2)
    val highestLenOfCol: Int = getHighestLength(sortOrder.map(_._1))
    val highestOfColumnNum: Int = sortOrder.map(_._2).max + 1
    val highLengthOfDefault: Int = getHighestLength(defaultMap.values.toSeq)
    val highestLengthFormat: Int = getHighestLength(colNotesMap.values.toSeq)
    var recordLength: Int = 0
    val extractLayout: Seq[(Int, String)] = sortOrder.map(line => {
      val lineNum: Int = line._2
      val colName: String = s"${line._1.trim.padTo(highestLenOfCol, ' ')}"
      val lengths: Array[String] = lengthMap.getOrElse(line._1, line._1).split(",")
      val (dataType, lengthOfCol): (String, Int) = layTypeConversion(dataTypeMap.getOrElse(line._1, line._1), lengths.head, lengths.last)
      recordLength = recordLength + lengthOfCol
      val field_default: String = getRightPad(defaultMap.getOrElse(line._1, line._1), highLengthOfDefault, "DEFAULT : ")
      val field_notes: String = getRightNotes(colNotesMap.getOrElse(line._1, line._1), highestLengthFormat, "FORMAT : ").toUpperCase
      (lineNum, s"$colName$dataType$field_default$field_notes")
    }) ++ Seq((highestOfColumnNum, s"*RECORD_COUNT $extractCount"), (highestOfColumnNum + 2, "*END")) ++ Seq((highestOfColumnNum + 1, s"*RECORD_LENGTH $recordLength"))
    val df: DataFrame = spark.sparkContext.parallelize(extractLayout).sortByKey(ascending = true).toDF("REC_SEQ", "REC_LAY")
    df
  }

  private def getHighestLength(arrayFields: Seq[String]): Int = {
    arrayFields.map(x => if (x == null) 0 else x.trim.length).toSet.max + 3
  }

  private def getRightPad(field: String, i: Int, notes: String): String = {
    if (field == null || field.trim == "SPACE") " ".padTo(i + notes.length, ' ') else s"$notes${field.trim.padTo(i, ' ')}"
  }

  private def getRightNotes(field: String, i: Int, notes: String): String = {
    if (field == null || field.trim == " ") " ".padTo(i + notes.length, ' ') else s"$notes${field.trim.padTo(i, ' ')}"
  }

  private def layTypeConversion(dataType: String, length1: String, scale1: String): (String, Int) = {
    val length: Int = if (length1.trim.toInt.isValidInt) length1.trim.toInt else 0
    if (dataType.toLowerCase.trim.contains("decimal") || dataType.toLowerCase.trim.contains("double") || dataType.toLowerCase.trim.contains("float")) {
      val scale: Int = if (scale1.trim.toInt.isValidInt) scale1.trim.toInt else 0
      (f"S$length%02d$scale%02d", length + scale + 1)
    } else if (dataType.toLowerCase.trim.contains("int") || dataType.toLowerCase.trim.contains("num") || dataType.toLowerCase.trim.contains("bigint") || dataType.toLowerCase.trim.contains("long")) {
      (f"$length%02d${"00"}", length)
    } else {
      (f"T$length%04d", length)
    }
  }


  def getTriggerFile(df: DataFrame, fileName: String, process_date: String, hdfsPath: String): Unit = {
    logger.info(s"get trigger file")
    val timeStamp: String = new SimpleDateFormat("yyyyMMddHHmmss").format(java.util.Calendar.getInstance().getTime)
    val extractName: String = s"${fileName}_${process_date}_$timeStamp.trg"
    val hdfsFilePath: String = s"$hdfsPath/trg"
    val pairRdd: RDD[(Long, String)] = df.repartition(1).orderBy(col("REC_SEQ").asc).select("REC_LAY")
      .map(_.mkString).rdd.zipWithIndex().map(_.swap)
    val allSortedDf: DataFrame = pairRdd.repartition(1).sortByKey(ascending = true).values.toDF("REC_LAY")
    allSortedDf.repartition(1).write.format("text")
      .options(Map("compression" -> "uncompressed", "quote" -> "\u0000")).mode("overwrite").save(s"$hdfsFilePath/$fileName")
    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val filePath: Path = fs.globStatus(new Path(s"$hdfsFilePath/$fileName/part-*"))(0).getPath
    fs.rename(filePath, new Path(s"$hdfsFilePath/$fileName/$extractName"))
  }
}
