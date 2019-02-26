name := "streaming"

version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.2.0",
                            "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
                            "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
                            "org.apache.spark" % "spark-mllib_2.11" % "2.2.0",
                            "com.databricks" % "spark-csv_2.11" % "1.5.0",
			    "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0")
