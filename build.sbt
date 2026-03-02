ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "ecg-streaming-classifier",
    idePackagePrefix := Some("it.utiu.thesis")
  )

val sparkVersion = "4.1.1"
val deltaVersion = "4.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,

  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  "io.delta" %% "delta-spark" % deltaVersion,
)