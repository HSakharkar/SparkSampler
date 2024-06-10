ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "OpenSparkSampler"
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.13" % "3.5.1"
libraryDependencies += "org.apache.spark" % "spark-sql-api_2.13" % "3.5.1"
libraryDependencies += "org.apache.spark" % "spark-hive_2.13" % "3.5.1"