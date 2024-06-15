package org.open.spark.sampler.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.open.spark.sampler.SamplerConverter._
import org.scalatest.funsuite.AnyFunSuite

class SamplerSuite  extends  AnyFunSuite {

  val conf = new SparkConf().setMaster("local[*]")
    .setAppName("SparkConf") // Replace with your application name

  implicit val spark_session: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()


  val personDF = spark_session.read.csv("src/test/resources/PersonData.txt").toDF("id", "name", "age", "gender", "size")
  val customColumnPersonDF = personDF.withColumn("custom_column", org.apache.spark.sql.functions.concat(personDF.col("gender"), personDF.col("size")))

  val sampleDF = customColumnPersonDF.sampleBy("custom_column", 40)

  test("An default sampling should have size 40") {
    println("sampleCount: " + sampleDF.count())
    assert(sampleDF.count() == 40)
  }

  val mapSampleDF = customColumnPersonDF.sampleBy("custom_column", 10, Map("MS" -> 0.2, "FM" -> 0.2, "FX" -> 0.3, "ML" -> 0.3))
  println("mapSampleCount: " + mapSampleDF.count())
  mapSampleDF.show(10)

  test("An male and small size combination should have be size 2") {
    val msDF = mapSampleDF.sampleBy("custom_column", 5, Map("MS" -> 1.0))
    assert(msDF.count() == 2)
  }

  test("An female and medium size combinations should have be size 2") {
    val fmDF = mapSampleDF.sampleBy("custom_column", 5, Map("FM" -> 1.0))
    assert(fmDF.count() == 2)
  }

  test("An female and extra size combination should have size 3") {
    val fxDF = mapSampleDF.sampleBy("custom_column", 5, Map("FX" -> 1.0))
    assert(fxDF.count() == 3)
  }

  test("An male and large size combination should have size 3") {
    val mlDF = mapSampleDF.sampleBy("custom_column", 5, Map("ML" -> 1.0))
    assert(mlDF.count() == 3)
  }


}
