name := "sortBytesScala"

version := "1.2.0"

scalaVersion := "2.11.12"
val sparkVersion = "2.1.0"

dependencyOverrides ++= Seq(
  "io.netty" % "netty" % "3.9.9.Final",
  "commons-net" % "commons-net" % "2.2",
  "com.google.guava" % "guava" % "11.0.2"
)

libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.13"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion 
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
