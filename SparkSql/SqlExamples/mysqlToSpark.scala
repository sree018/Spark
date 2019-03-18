import org.apache.spark.sql.SparkSession

object mysqlToSpark extends App {
  val spark = SparkSession.builder().appName("loading data from mysql").master("local[*]").getOrCreate()
 // Class.forName("com.mysql.jdbc.Driver").newInstance
  val mysql_car_databases= spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/dataSources")
                                  .option("dbtable","CarData")
                                  .option("user","root").option("password","----")
                                  .load().createOrReplaceGlobalTempView("cars")
         spark.sql("select * from cars").show()
   
                                  
  
}
