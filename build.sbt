name := "AirlineViews"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.1"
val hadoopVersion = "2.8.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.spark" %% "spark-yarn" % sparkVersion,
  "org.apache.hadoop" % "hadoop-hdfs" %hadoopVersion
)