import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.databricks.spark.csv
import org.apache.spark.sql.DataFrame

object AnalysisOfYelpData {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("analysis of yelp Json data").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
        val data = sqlContext.read.json("/home/srinu/Desktop/yelpData/yelp_academic_dataset_business.json")
				.registerTempTable("business")
    val query ="""select attributes.WiFi from business"""
    val data1 = sqlContext.sql(query).show()
  }
}
