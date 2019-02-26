import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.ml.feature.{HashingTF,IDF, Tokenizer}

 
object Utils {
   val numFeatures = 1000
  val tf = new HashingTF("numFeatures")
   def featurize(s: String): Vector = {
     val n = 1000
     val result = new Array[Double](n)
     val bigrams = s.sliding(2).toArray
     for (h <- bigrams.map(_.hashCode % n)){
       result(h) += 1.0/bigrams.length
     }
    Vectors.sparse(n, result.zipWithIndex.filter(_._1 != 0).map(_.swap))
  }
   
}

object K_means {
 def main(args:Array[String]){
  val conf = new SparkConf().setAppName("k-means").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
  val texts = sc.textFile("/home/srinu/Documents/usatotal").map(x=>x.split("\t")(2))
 val numClusters = 3
 val vectors = texts.map(Utils.featurize).cache()
 val model = KMeans.train(vectors, numClusters, 30)
 model.clusterCenters.foreach(println)
  val some_tweets = texts.take(100)
 for (i <- 0 until numClusters) {
  println(s"\nCLUSTER $i:")
  some_tweets.foreach { t =>
    if (model.predict(Utils.featurize(t)) == i) {
      println(t)
    }
  }
}
 
 
  }
 }
