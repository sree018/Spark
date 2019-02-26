


import org.apache.spark.sql.SparkSession

case class Route(fltIta: String, fltId: String, srcCode: String, portID: String, destCode: String, destPortId: String, codeShare: String, stops: Int, fltType: String)
case class Airline(fltId: String, fltName: String, fltCode: String, itaCodePort: String, icaoCode: String, fltAName: String, hub: String, active: String)
case class Airport(PortId:Int, NameOfPort:String, PortOfCity:String, Country:String,IATA_FAA_Code:String, ICAO_Code:String, Latitude:Float,Longitude:Float,Altitude:Int,TimeZone:Float,DST:String,TZ:String)


object analysisOfData{
  def main(args:Array[String]): Unit ={
   val spark = SparkSession.builder().appName("analysis of AirlinesData").master("local[*]").getOrCreate()

    val routes = spark.sparkContext.textFile("/home/Desktop/Air-datasets/routes.dat")
    val routesData = routes.map(getRoute).foreach(println)
   
    val airline = spark.sparkContext.textFile("/home/Desktop/Air-datasets/Final_airlines")
    val airlineData = airline.map(getAirline).toDF().registerTempTable("AIRLINES")

    val airports = spark.sparkContext.textFile("/home/Desktop/Air-datasets/airports_mod.dat")
    val airportsData = airports.map(getAirport).toDF().registerTempTable("AIRPORTS")

    //Find list of Airports operating in the Country India
    val queryA = " select NameOfPort from AIRPORTS where Country == 'India' "
    spark.sql(queryA).rdd.saveAsTextFile("/home/Desktop/A")


    //Find the list of Airlines having zero stops
    val queryB = "select  distinct A.fltName FROM ROUTES R INNER JOIN AIRLINES A on R.fltId = A.fltId where R.stops == 0 "
    spark.sql(queryB).rdd.saveAsTextFile("/home/Desktop/B")

    //List of Airlines operating with code share
    val queryC = "select distinct A.fltName from  ROUTES R  INNER JOIN AIRLINES A ON R.fltId = A.fltId where R.codeShare=='Y' "
    spark.sql(queryC).rdd.saveAsTextFile("/home/Desktop/C")

    //Which country (or) territory having highest Airports     
    val queryD = "SELECT hub, count(distinct icaoCode) as cnt from AIRLINES GROUP by hub order by cnt desc"
    spark.sql(queryD).rdd.saveAsTextFile("/home/Desktop/D")

    //Find the list of Active Airlines in United state 
    val queryE = "select distinct A.fltName from  ROUTES R  INNER JOIN AIRLINES A ON R.fltId = A.fltId where R.codeShare=='Y' "
    spark.sql(queryE).rdd.saveAsTextFile("/home/Desktop/E")
  }
  def getAirport(line: String): Airport = {
    val cols = line.split(",", -1);
    val PortId = cols(0).toInt
    val NameOfPort = cols(1)
    val PortOfCity = cols(2)
    val Country = cols(3)
    val IATA_FAA_Code = if (cols(4).equals("")) "NA" else cols(4)
    val ICAO_Code = if (cols(5).equals("\\N")) "NA" else cols(5)
    val Latitude = cols(6).toFloat
    val Longitude = cols(7).toFloat
    val Altitude = cols(8).toInt
    val TimeZone = cols(9).toFloat
    val DST = cols(10)
    val TZ = if (cols(11).equals("\\N")) "NA" else cols(11)
    val airport = Airport(PortId, NameOfPort, PortOfCity, Country,IATA_FAA_Code, ICAO_Code, Latitude,Longitude,Altitude,TimeZone,DST,TZ )
    (airport)
  }

  def getRoute(line: String): Route = {
    val cols = line.split(",", -1);
    val fltIta = cols(0)
    val fltId = if (cols(1).equals("\\N")) "Nan" else cols(1)
    val srcCode = cols(2)
    val artId = cols(3)
    val destCode = cols(4)
    val destPortId = if (cols(5).equals("\\N")) "NA" else cols(5)
    val codeShare = if (cols(6).equals("")) "NA" else cols(6)
    val stops = cols(7).toInt
    val fltType = if (cols(8).equals("")) "NA" else cols(8)
    val route = Route(fltIta, fltId, srcCode, artId, destCode, destPortId, codeShare, stops, fltType)
    (route)
  }

  def getAirline(line: String): Airline = {
    val cols = line.split(",", -1);
    val fltId = cols(0)
    val fltName = cols(1)
    val fltCode = if (cols(2).equals("\\N")) "NA" else cols(2)
    val itaCodePort = cols(3)
    val icaoCode = cols(4)
    val fltAName = cols(5)
    val hub = cols(6)
    val active = cols(7)
    val airline = Airline(fltId, fltName, fltCode, itaCodePort, icaoCode, fltAName, hub, active)
    (airline)
  }


}
