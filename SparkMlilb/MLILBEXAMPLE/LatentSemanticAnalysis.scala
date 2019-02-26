import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.collection.Map
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import scala.io.Source
import com.google.inject.ImplementedBy
import math.log
import java.io._
import org.apache.lucene.search.TermScorer
import org.apache.lucene.search.TermScorer

class Tailgates extends Serializable {
  
def createNLPPipeline(): StanfordCoreNLP = {
     val props = new java.util.Properties()
     props.put("annotators", "tokenize, ssplit, pos, lemma")
     new StanfordCoreNLP(props)
      }

 def isOnlyLetters(str: String): Boolean = str forall Character.isLetter
 
 def TextToLemmas(tweet: String, stopWords: Set[String], pipline: StanfordCoreNLP): Seq[String] = {
     val doc = new Annotation(tweet)
     pipline.annotate(doc)
     val lemmas = new ArrayBuffer[String]()
     val sentences = doc.get(classOf[SentencesAnnotation]).asScala
     for {
         sentence <- sentences
         token <- sentence.get(classOf[TokensAnnotation]).asScala
         } {
         val lemma = token.get(classOf[LemmaAnnotation])
         if (lemma.length > 2 && !stopWords(lemma) && isOnlyLetters(lemma)) {
             lemmas += lemma.toLowerCase
          }
        }
      lemmas
    }
 def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numTerms: Int, termIds: Map[Int,String]): Seq[Seq[(String, Double)]] = {
        val v = svd.V
        val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
      val arr = v.toArray
      for (i <- 0 until numConcepts) {
        val offs = i * v.numRows
        val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
        val sorted = termWeights.sortBy(-_._1)
        topTerms += sorted.take(numTerms).map {
            case (score, id) => (termIds(id), score) 
        }
      }
      topTerms
  }
def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u  = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
      topDocs += docWeights.top(numDocs).map { case (score, id) => (docIds(id), score) }
    }
    topDocs
    }

}
  
  
object RunLSA {
  def main(args: Array[String]){
     val conf = new SparkConf().setAppName("lda").setMaster("local[2]")
     val sc: SparkContext = new SparkContext(conf)
     val data = sc.textFile("/home/srinu/Desktop/indiafood/india")//.map(x=>x.split("\t")(1))
     val stopWords = sc.broadcast(Source.fromFile("/home/srinu/Public/stopwords").getLines.toSet).value
     val m = new Tailgates() 
     val pipeline = m.createNLPPipeline()    
     val lemmatized: RDD[Seq[String]] = data.mapPartitions { iter => 
        val pipeline = m.createNLPPipeline()
     iter.map { case (tweet) => m.TextToLemmas(tweet, stopWords, pipeline) }
       }
     val docTermFreqs = lemmatized.map ( terms => {
         val termFreqs = terms.foldLeft(new HashMap[String, Int]()) {
           (map, term) => { 
             map += term -> (map.getOrElse(term, 0) + 1)
             map
             }
           }
          termFreqs
              })
      val zero = new HashMap[String, Int]()
      def merge(dfs: HashMap[String, Int], tfs: HashMap[String, Int])
        : HashMap[String, Int] = {
       tfs.keySet.foreach { term => 
          dfs += term -> (dfs.getOrElse(term, 0) + 1)
       }   
     dfs
     }
    def comb(dfs1: HashMap[String, Int], dfs2: HashMap[String, Int])
      : HashMap[String, Int] = {
       for((term, count) <- dfs2) {
          dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
       }
      dfs1
     }
     docTermFreqs.aggregate(zero)(merge, comb)
     val docFreq = docTermFreqs.flatMap(_.keySet).map((_,1)).reduceByKey(_ + _)
     val numDocs = docTermFreqs.flatMap(_.keySet).distinct.count
     val docIds = docTermFreqs.flatMap(_.keySet).zipWithUniqueId().map(_.swap).collectAsMap()
     val numTerms = 5000
       implicit val ordering = Ordering.by[(String, Int), Int](_._2)
    val topDocFreqs = docFreq.top(numTerms)(ordering)
    val idfs = docFreq.map {
        case (term, count) => (term, math.log(numDocs.toDouble / count))
      }.collectAsMap()
    val idTerms = idfs.keys.zipWithIndex.toMap
    val termIds = idTerms.map(_.swap)
    val bIdfs = sc.broadcast(idfs).value
    val bIdTerms = sc.broadcast(idTerms).value
    val termDocMatrix = docTermFreqs.map ( termFreqs => {
         val docTotalTerms = termFreqs.values.sum
         val termScores = termFreqs.filter {
             case (term, freq) => bIdTerms.contains(term)
         }.map {
             case (term, freq) => (bIdTerms(term),
                 bIdfs(term) * termFreqs(term) / docTotalTerms)
         }.toSeq
        Vectors.sparse(bIdTerms.size, termScores)
          })
    val mat = new RowMatrix(termDocMatrix)
    val k = 100
    val svd = mat.computeSVD(k, computeU=true)
    val topConceptTerms =m.topTermsInTopConcepts(svd, 10,10, termIds)
     val topConceptDocs =m.topDocsInTopConcepts(svd, 10, 10, docIds)
         for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
          println ("Concept terms: " + terms.map(_._1).mkString(","))
         println()
         }  
  
      }
  }
