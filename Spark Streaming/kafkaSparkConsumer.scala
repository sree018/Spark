import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.streaming.kafka010.{ ConsumerStrategies, LocationStrategies }
import org.apache.spark.streaming.kafka010.KafkaUtils

object Consumer extends App {
  val conf = new SparkConf().setAppName("consumer application").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(10))
  ssc.sparkContext.setLogLevel("WARN")
  val kafkaParams = Map[String, String]("bootstrap.servers" -> "localhost:9092", "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer","group.id" -> "consumergroup-1", "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", "auto.offset.reset" -> "latest")
  val topics = Set("ranking")
  
  val inputKafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
  val states = inputKafkaStream.transform { rdd =>
    rdd.map(record => (record.value().toString))
  }
  states.print()
  ssc.start()
  ssc.awaitTermination()
}
