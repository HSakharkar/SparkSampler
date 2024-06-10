/*
 * Copyright 2024 Hemant Sakharkar

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.open.spark.sampler.example

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.open.spark.sampler.SamplerConverter._

object MainSampler {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("SparkConf") // Replace with your application name

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    implicit val sc = spark

    val person = spark.read.csv("/Users/hemantsakharkar/helloHemant/data/Wide2gb/person/PersonData.txt").toDF("id", "name", "age", "gender", "product", "c5", "c6", "size")
    val addedPerson = person.withColumn("custom_column", org.apache.spark.sql.functions.concat(person.col("gender"), person.col("size")))

    val rperson = addedPerson.select("id", "name", "age", "gender", "size", "custom_column")

    rperson.show(10)
    val t1 = System.currentTimeMillis()
    val sampleDF = addedPerson.sampleBy("custom_column", 10000)

    println(sampleDF.count()+ " time:"+(System.currentTimeMillis() - t1))
    sampleDF.show(10)


  }

}
