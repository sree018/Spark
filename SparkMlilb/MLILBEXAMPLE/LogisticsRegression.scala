import org.apache.spark.sql.{SparkSession,Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.classification.LogisticRegression

object Cart {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("analysis").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    def Gender = udf(( x: String ) => {
              x match {
        case "female" => 1.0
        case _ => 0.0
      }
    })


        val trainData = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true")
                                .load("/home/srinu/titanic/train.csv")
                                .na.drop()
                                .select("Survived", "PassengerId", "Pclass", "Age", "SibSp", "Parch", "sex")
                                .withColumn("sex", Gender('sex))

        val DataPoints = trainData.map { row =>
         new LabeledPoint(row.getInt(0).toDouble,Vectors.dense(row.getInt(1).asInstanceOf[Double],row.getInt(2).asInstanceOf[Double],row.getAs(3),row.getInt(4).asInstanceOf[Double],row.getInt(5).asInstanceOf[Double],row.getAs(6))

         )}.cache()


    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    // Fit the model
    val lrModel = lr.fit(DataPoints)


    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")


  }

}
