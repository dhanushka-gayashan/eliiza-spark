name := "eliiza-spark"

version := "0.1"

scalaVersion := "2.12.10"

idePackagePrefix := Some("com.kdg.spark")

val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  //spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)