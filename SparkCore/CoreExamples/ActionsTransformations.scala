
  
package com.sparkPrepartions


  import org.apache.spark.{ SparkConf, SparkContext }
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.DataFrame

  case class userFrame(first_name: String, Last_name: String, address: String,
                       Country: String, City: String, state: String, PostCode: String, Phone1: String, Phone2: String, email: String, web: String)
class CoreExamples {
    def transformations (rdd: RDD[String]): RDD[userFrame] = {
      val data2 = rdd.map(line => line.split(",")).map(x => userFrame(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10)))
      val header = data2.first()
      val data4 = data2.filter(row => row != header)
      return data4
    }
    def actions(rec: DataFrame): Unit = {
      val frame = rec.groupBy("Country").count().show()
    }

  }

  object ActionsTransformations {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("sparkcertifications").setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._
      val data = sc.textFile("/home/srinu/datasets/UserRecords.txt")
      val connection = new training()
      val data2 = connection.transformations(data).toDF()
      val data3 = connection.actions(data2)
    }
  }
