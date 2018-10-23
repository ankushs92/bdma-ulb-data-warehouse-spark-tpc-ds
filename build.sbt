name := "db-warehouse-tpc-ds-spark"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.2"
val scalaLoggingVersion = "3.9.0"
val logbackVersion = "1.2.3"
val mysqlVersion = "5.1.16"
val openCsvVersion = "4.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  "mysql" % "mysql-connector-java" % mysqlVersion,
  "com.github.tototoshi" %% "scala-csv" % "1.3.5"
)

