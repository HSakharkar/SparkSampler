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
package org.open.spark.sampler

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, DataFrameStatFunctions, Row, SparkSession}

/**
 * class to add DataFrame Functions for creating a samples from Spark Dataframe
 */
class DataFrameStatsFunctionsSampler (df: DataFrame)(implicit  spark_session: SparkSession) extends Logging{

  /**
   * Gets a representative samples from spark dataframe for a column based on given sample size
   * @param col Column to be used
   * @param sampleSize sample size required from a dataframe
   * @param contributions contribution Map which provide percentage required in a required samples
   * @return
   */
  def sampleBy(col: Column, sampleSize: Int): DataFrame =  {
    sampleBy(col.toString(), sampleSize)
  }

  /**
   * Gets a representative samples from spark dataframe for a column based on given sample size
   * @param col String column name
   * @param sampleSize sample size required from a dataframe
   * @param contributions contribution Map which provide percentage required in a required samples
   * @return
   */
  def sampleBy(col: String, sampleSize:Int, contributions:Map[String, Double]=Map.empty[String,Double]): DataFrame = {

    val sampleRdd = Sampler().ofRDD(df.rdd).sparkContext(spark_session.sparkContext).customKeys((t) => {

      val r = t.asInstanceOf[Row]
      r.getString(r.fieldIndex(col))

    }).sampleSize(sampleSize).contributions(contributions).createSample()

    val sampleRDDRow = sampleRdd.asInstanceOf[RDD[Row]]
    val sampleDf = spark_session.createDataFrame(sampleRDDRow, df.schema)
    sampleDf
  }

}

/**
 *
 * Sample Converter to add implicit functions to Dataframe class
 */
object SamplerConverter {
  implicit def asDF(df : DataFrame)(implicit spark_session: SparkSession) =
    new DataFrameStatsFunctionsSampler(df)
}
