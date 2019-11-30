package com.spark.rdbms.utils

import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import java.io.File
import org.apache.spark.sql.SparkSession
import scala.util.Properties
import java.util.Properties
import java.io.FileReader
import scala.io.Source
import org.apache.spark.sql._
import java.io.FileInputStream
import scala.collection.JavaConverters._

/*
 *
 * @author srinu
 *
 */
class JdbcUtils extends Serializable {
  val logger = Logger.getLogger(this.getClass)

  logger.info("Reading application aruguments")

  def configFile(fileArgs: String): Properties = {
    val props = new Properties()
    props.load(new FileReader(new File(fileArgs)))
    props
  }

  def sparkSession(appName: String, appMaster: String, hiveWarehouse: String, hiveSchema: Boolean): SparkSession = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark: SparkSession = hiveSchema match {
      case true => {

        val spark1 = SparkSession.builder()
          .appName(appName)
          .master(appMaster)
          .config("spark.sql.warehouse.dir", warehouseLocation)
          .enableHiveSupport()
          .getOrCreate()
        logger.info("====initializing spark session with Hive ====")
        spark1.sparkContext.setLogLevel("warn")
        import spark1.implicits._
        spark1
      }
      case false => {

        val spark1 = SparkSession.builder()
          .appName(appName)
          .master(appMaster)
          .getOrCreate()
        logger.info("====initializing spark session withOut Hive ====")
        import spark1.implicits._
        spark1.sparkContext.setLogLevel("warn")
        spark1
      }
    }
    spark
  }

  def srDbUtils(parameters: Properties): Properties = {
    logger.info("====reading source database connections=====")
    val timezone = s"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val jdbcUser = parameters.getProperty("source.db.jdbc.userName")
    val jdbcPass = parameters.getProperty("source.db.jdbc.password")
    val jdbcDriver = parameters.getProperty("source.db.jdbc.driver")
    val jdbcHostName = parameters.getProperty("source.db.jdbc.hostName")
    val jdbcDataBase = parameters.getProperty("source.db.jdbc.database")
    val jdbcPort = parameters.getProperty("source.db.jdbc.port").toInt
    val dbType = parameters.getProperty("source.db.type")
    val jdbcUrl = s"jdbc:${dbType}://${jdbcHostName}:${jdbcPort}/${jdbcDataBase}${timezone}"
    val dbUtils = new Properties()
    dbUtils.getProperty("driver", jdbcDriver)
    dbUtils.put("user", s"${jdbcUser}")
    dbUtils.put("password", s"${jdbcPass}")
    dbUtils.put("url", s"${jdbcUrl}")
    dbUtils

  }
  def loadSrcDbData(spark: SparkSession, parameters: Properties, dbConnProps: Properties): DataFrame = {
    logger.info("====connecting to jdbc and fetch required data from JDBC====")
    val partitionColumn = parameters.getProperty("source.db.jdbc.partitionColumn")
    val dbSqlString = parameters.getProperty("source.db.jdbc.sql")
    val lowerBound = parameters.getProperty("source.db.jdbc.lowerbound").toInt
    val upperBound = parameters.getProperty("source.db.jdbc.upperbound").toInt
    val numPartitions = parameters.getProperty("source.db.jdbc.numPartitions").toInt
    val dbSchemaString = parameters.getProperty("source.db.jdbc.schema")
    val dbSql = scala.io.Source.fromFile(dbSqlString).mkString
    val dbSql1 = dbSql.replaceAll("dbSchema", dbSchemaString)
    val sql = "(" + dbSql1 + ") temp_table"
    println("input sql:   " + dbSql1)
    if (partitionColumn != null) {
      logger.info("====partitionColumn is  " +partitionColumn+"======")
      val dbData = spark.read.jdbc(dbConnProps.getProperty("url"), sql, dbConnProps)
      dbData.show(10,false)
      dbData
    } else {
      logger.info("====partitionColumn is  " +partitionColumn+"======")
      val dbData = spark.read.jdbc(
        dbConnProps.getProperty("url"),
        sql,
        partitionColumn,
        lowerBound,
        upperBound,
        numPartitions,
        dbConnProps)
        
        dbData.show(15,false)
      dbData
    }
  }
  def sinkDbConnections(filePath: String): Properties = {
    val timezone = s"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val props = new Properties()
    props.load(new FileInputStream(filePath))
    val jdbcHostName = props.getProperty("source.db.jdbc.hostName")
    val jdbcDataBase = props.getProperty("source.db.jdbc.database")
    val jdbcPort = props.getProperty("source.db.jdbc.port").toInt
    val dbType = props.getProperty("source.db.type")
    val jdbcUrl = s"jdbc:${dbType}://${jdbcHostName}:${jdbcPort}/${jdbcDataBase}${timezone}"
    println(jdbcUrl)
    val jdbcDriver = props.getProperty("sink.db.jdbc.driver")
    val jdbcUser = props.getProperty("sink.db.jdbc.userName")
    val jdbcPassword = props.getProperty("sink.db.jdbc.password")
    val dbUtils = new Properties()
    dbUtils.getProperty("Driver", s"{jdbcDriver}")
    dbUtils.put("user", s"{jdbcUser}")
    dbUtils.put("password", s"{jdbcPassword}")
    dbUtils.put("url", s"{jdbcUrl}")
    dbUtils
    /*
 * source.db.type=mysql
source.db.jdbc.hostName=localhost
source.db.jdbc.database=testDataBase
source.db.jdbc.port=3306
 */
  }
  //  def getInsertStatement(table: String,df:DataFrame,tableSchema: Option[StructType],isCaseSensitive: Boolean,dialect: JdbcDialect,keyColumns:List[String]):String={
  //    val columns = df.columns.map(x => dialect.quoteIdentifier(x)).mkString(",")
  //    val placeholders = df.schema.map(_ => "?").mkString(",")
  //    val pkColumns = keyColumns.map(_ => "?").mkString
  //    val insert = s"INSERT INTO $table ($columns) VALUES ($placeholders) where ($pkColumns)"
  //    insert
  //  }
  //
  //
  //  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
  //    dt match {
  //      case IntegerType   => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
  //      case LongType      => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
  //      case DoubleType    => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
  //      case FloatType     => Option(JdbcType("REAL", java.sql.Types.FLOAT))
  //      case ShortType     => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
  //      case ByteType      => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
  //      case BooleanType   => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
  //      case StringType    => Option(JdbcType("TEXT", java.sql.Types.CLOB))
  //      case BinaryType    => Option(JdbcType("BLOB", java.sql.Types.BLOB))
  //      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
  //      case DateType      => Option(JdbcType("DATE", java.sql.Types.DATE))
  //      case t: DecimalType => Option(
  //        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
  //      case _ => None
  //    }
  //  }
  //
  //  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
  //    dialect.getJDBCType(dt).orElse(getCommonJDBCType(dt)).getOrElse(
  //      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.catalogString}"))
  //  }
  //
  // def saveTable(df: DataFrame,tableSchema: Option[StructType],isCaseSensitive: Boolean,dbProps:Properties,sinDb:(Properties,Connection),keyColumns:List[String]): Unit = {
  //    val lowerBound = dbProps.getProperty("sink.db.jdbc.lowerbound").toInt
  //    val upperBound = dbProps.getProperty("sink.db.jdbc.upperbound").toInt
  //    val partitionColumn = dbProps.getProperty("sink.db.jdbc.partitionColumn")
  //    val numPartitions = dbProps.getProperty("sink.db.jdbc.numPartitions").toInt
  //    val numPartitionsSize = dbProps.getProperty("sink.db.jdbc.numPartitions.size").toInt
  //    val table = dbProps.getProperty("sink.db.name")
  //     val driver =dbProps.getProperty("driver",s"{jdbcDriver}" )
  //    val rddSchema =df.schema
  //    val dbProperties =sinDb._1
  //    val dbConnection =sinDb._2
  //    val dialect = JdbcDialects.get(dbProperties.getProperty("url"))
  //    Class.forName("driver").newInstance();
  //    val insertStmt = getInsertStatement(table, df, tableSchema, isCaseSensitive, dialect,keyColumns)
  //    val repartitionedDF = numPartitions match {
  //      case numPartitions  if(numPartitions<= 0) => throw new IllegalArgumentException(s"Invalid value `$numPartitions` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " + "via JDBC. The minimum value is 1.")
  //      case numPartitions  if(numPartitions < df.rdd.getNumPartitions) => df.coalesce(numPartitions)
  //      case _ => df
  //    }
  //    repartitionedDF.rdd.foreachPartition { iterator => savePartition(dbConnection, table, iterator, rddSchema, insertStmt, numPartitionsSize, dialect, isolationLevel, options)}
  //  }
  //
  // def savePartition(dBConnection:Connection,table: String,iterator: Iterator[Row], rddSchema: StructType,insertStmt: String, batchSize: Int,dialect: JdbcDialect,isolationLevel: Int,options: JDBCOptions): Unit = {
  //    val outMetrics = TaskContext.get().taskMetrics().outputMetrics
  //    val conn = dBConnection
  //    var committed = false
  //    var finalIsolationLevel = Connection.TRANSACTION_NONE
  //    if (isolationLevel != Connection.TRANSACTION_NONE) {
  //      try {
  //        val metadata = conn.getMetaData
  //        if (metadata.supportsTransactions()) {
  //          // Update to at least use the default isolation, if any transaction level
  //          // has been chosen and transactions are supported
  //          val defaultIsolation = metadata.getDefaultTransactionIsolation
  //          finalIsolationLevel = defaultIsolation
  //          if (metadata.supportsTransactionIsolationLevel(isolationLevel))  {
  //            // Finally update to actually requested level if possible
  //            finalIsolationLevel = isolationLevel
  //          } else {
  //            logger.warn(s"Requested isolation level $isolationLevel is not supported; " + s"falling back to default isolation level $defaultIsolation")
  //          }
  //        } else {
  //          logger.warn(s"Requested isolation level $isolationLevel, but transactions are unsupported")
  //        }
  //      } catch {
  //        case NonFatal(e) => logger.warn("Exception while detecting transaction support", e)
  //      }
  //    }
  //    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE
  //    var totalRowCount = 0L
  //    try {
  //      if (supportsTransactions) {
  //        conn.setAutoCommit(false) // Everything in the same db transaction.
  //        conn.setTransactionIsolation(finalIsolationLevel)
  //      }
  //      val stmt = conn.prepareStatement(insertStmt)
  //      val setters = rddSchema.fields.map(f => makeSetter(conn, dialect, f.dataType))
  //      val nullTypes = rddSchema.fields.map(f => getJdbcType(f.dataType, dialect).jdbcNullType)
  //      val numFields = rddSchema.fields.length
  //
  //      try {
  //        var rowCount = 0
  //
  //        stmt.setQueryTimeout(options.queryTimeout)
  //
  //        while (iterator.hasNext) {
  //          val row = iterator.next()
  //          var i = 0
  //          while (i < numFields) {
  //            if (row.isNullAt(i)) {
  //              stmt.setNull(i + 1, nullTypes(i))
  //            } else {
  //              setters(i).apply(stmt, row, i)
  //            }
  //            i = i + 1
  //          }
  //          stmt.addBatch()
  //          rowCount += 1
  //          totalRowCount += 1
  //          if (rowCount % batchSize == 0) {
  //            stmt.executeBatch()
  //            rowCount = 0
  //          }
  //        }
  //        if (rowCount > 0) {
  //          stmt.executeBatch()
  //        }
  //      } finally {
  //        stmt.close()
  //      }
  //      if (supportsTransactions) {
  //        conn.commit()
  //      }
  //      committed = true
  //    } catch {
  //      case e: SQLException =>
  //        val cause = e.getNextException
  //        if (cause != null && e.getCause != cause) {
  //          // If there is no cause already, set 'next exception' as cause. If cause is null,
  //          // it *may* be because no cause was set yet
  //          if (e.getCause == null) {
  //            try {
  //              e.initCause(cause)
  //            } catch {
  //              // Or it may be null because the cause *was* explicitly initialized, to *null*,
  //              // in which case this fails. There is no other way to detect it.
  //              // addSuppressed in this case as well.
  //              case _: IllegalStateException => e.addSuppressed(cause)
  //            }
  //          } else {
  //            e.addSuppressed(cause)
  //          }
  //        }
  //        throw e
  //    } finally {
  //      if (!committed) {
  //        // The stage must fail.  We got here through an exception path, so
  //        // let the exception through unless rollback() or close() want to
  //        // tell the user about another problem.
  //        if (supportsTransactions) {
  //          conn.rollback()
  //        } else {
  //          outMetrics.setRecordsWritten(totalRowCount)
  //        }
  //        conn.close()
  //      } else {
  //        outMetrics.setRecordsWritten(totalRowCount)
  //
  //        // The stage must succeed.  We cannot propagate any exception close() might throw.
  //        try {
  //          conn.close()
  //        } catch {
  //          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
  //        }
  //      }
  //    }
  //  }

}



















































































































