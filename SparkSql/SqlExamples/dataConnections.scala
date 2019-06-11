package com.jdbcAnlysis
import java.util.Properties
import java.io.FileInputStream
class dataConnections {
  def dbConnections(filePath: String): java.util.Properties = {
    val timezone = s"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val prop = new Properties()
    val dbUtils = new Properties()
    prop.load(new FileInputStream(filePath))
    val HostName = prop.getProperty("jdbcHostName")
    val Port = prop.getProperty("jdbcPort")
    val DataBase = prop.getProperty("jdbcDataBase")
    val Url = s"jdbc:mysql://${HostName}:${Port}/${DataBase}${timezone}"
    dbUtils.getProperty("driver", prop.getProperty("driverClass"))
    dbUtils.put("user", prop.getProperty("jdbcUserName").trim)
    dbUtils.put("password", prop.getProperty("jdbcPassWord"))
    dbUtils.put("url", Url)
    return dbUtils

  }
}
