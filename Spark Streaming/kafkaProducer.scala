import scala.util.Random
import java.util.{ Date, Properties }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }


object Producer extends App {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
 
  val topics = "ranking";
  val rnd = new Random
  val low = 1
  val high = 50
  val states = List("ALABAMA", "ALASKA", "ARIZONA", "ARKANSAS", "CALIFORNIA", "COLORADO", "CONNECTICUT", "DELAWARE", "FLORIDA", "GEORGIA", "HAWAII", "IDAHO", "ILLINOIS", "INDIANA", "IOWA", "KANSAS", "KENTUCKY", "LOUISIANA", "MAINE", "MARYLAND", "MASSACHUSETTS", "MICHIGAN", "MINNESOTA", "MISSISSIPPI", "MISSOURI", "MONTANA", "NEBRASKA", "NEVADA", "NEW HAMPSHIRE", "NEW JERSEY", "NEW MEXICO", "NEW YORK", "NORTH CAROLINA", "NORTH DAKOTA", "OHIO", "OKLAHOM", "OREGON", "PENNSYLVANIA", "RHODE ISLAND", "SOUTH CAROLINA", "SOUTH DAKOTA", "TENNESSEE", "TEXAS", "UTAH", "VERMON", "VIRGINIA", "WASHINGTON", "WEST VIRGINIA", "WISCONSIN", "WYOMING")
  val producer = new KafkaProducer[String, String](props)
  while(true) {
        val state = states(rnd.nextInt(high -low)+low)
        val rank = (rnd.nextInt(high -low)+low)
        val record = rank + "\t" + state + "\t" + System.currentTimeMillis()
        val data = new ProducerRecord(topics, state, record)
        
        producer.send(data)
         Thread.sleep(10000)
         }
   producer.close()
}
