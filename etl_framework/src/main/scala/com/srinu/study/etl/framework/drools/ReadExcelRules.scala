package com.srinu.study.etl.framework.drools


import com.srinu.study.etl.framework.logging.Logging
import org.apache.poi.xssf.usermodel._
import org.apache.spark.sql.SparkSession
import org.apache.poi.ss.usermodel._

import java.io._
import scala.collection.JavaConverters._
import org.apache.spark.unsafe.types.UTF8String

class ReadExcelRules extends Logging {

  def getExcelData(spark: SparkSession, filePath: String, excelSheetName: String): String = {
    logger.info(s"parsing excel sheet name : $excelSheetName")
    val excelFile = new File(filePath)
    val excelWorkbook = new XSSFWorkbook(new FileInputStream(excelFile))
    val sheet: XSSFSheet = excelWorkbook.getSheet(excelSheetName)
    val seqOfRows: Seq[(Int, Seq[(String, Any)])] = getSheetData(sheet)
    val drlFile: String = getDrlFileFromExcel(seqOfRows)
    drlFile
  }

  def getSheetData(sheet: XSSFSheet): Seq[(Int, Seq[(String, Any)])] = {
    val firstRow: Int = sheet.getFirstRowNum
    val header: List[(String, Int)] = sheet.getRow(firstRow).asScala.
      zipWithIndex.map(cell=>(cell._1.getStringCellValue.trim,cell._2)).toList
    if (header.isEmpty) throw new Exception("No data found on first row and no header")
    val seqOfSheetRows: Seq[(Int, Seq[(String, Any)])] = sheet.asScala.zipWithIndex.filterNot(_._2 == 0).map(row => {
      val index: Int = row._2
      val currentRow: XSSFRow = sheet.getRow(index)
      val cellData: Seq[(String, Any)] = header.indices.map(index => {
        val cellKey = header(index)._1
        val cell = currentRow.getCell(index, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)
        val cellValue:Any = if (cell == null) {
          ""
        } else {
          cell.getCellType match {
            case CellType.NUMERIC => cell.getNumericCellValue.toInt
            case _ => cell.getStringCellValue
          }
        }
        (cellKey, cellValue)
      })
      (index, cellData)
    }).toSeq
    seqOfSheetRows
  }

  def getDrlFileFromExcel(seqOfRows: Seq[(Int, Seq[(String, Any)])]): String = {
    val sb: StringBuilder = new StringBuilder()
    sb.append("import com.srinu.spark.etl.farmework.drools \n\n")
    sb.append("dialect \"mvel\" \n\n")
    seqOfRows.foreach(line => {
      line._2.foreach(data => {
        val ruleName: String = data._1.trim.toUpperCase
        if (ruleName == "RULENAME") {
          sb.append(s"""rule \"${data._2}\" \n""")
        } else if (ruleName == "SALIENCE") {
          if (data._2.toString.trim.nonEmpty) sb.append(s"""   SALIENCE ${data._2} \n""")
        } else if (ruleName == "CONDITION") {
          sb.append(s"      when \n")
          sb.append(s"         ${data._2} \n")
        } else{
          sb.append(s"      then \n")
          sb.append(s"         ${data._2} \n\n")
          sb.append(s" end")
        }
      })
    })
    sb.mkString
  }
}
