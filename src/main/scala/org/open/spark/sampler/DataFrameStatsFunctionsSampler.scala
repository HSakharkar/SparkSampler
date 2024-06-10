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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.{Column, DataFrame, DataFrameStatFunctions, Row, SparkSession}
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin

class DataFrameStatsFunctionsSampler (df: DataFrame)(implicit  sc: SparkSession){

  def sampleBy(col: Column, sampleSize: Int): DataFrame =  {
    sampleBy(col.toString(), sampleSize)
  }

  def sampleBy(col: String, sampleSize: Int): DataFrame =  {

    val sampleRdd = Sampler.apply().ofRDD(df.rdd).sparkContext(sc.sparkContext).customKeys((t) => {

      val r = t.asInstanceOf[Row]
      r.getString(r.fieldIndex(col))

    }).sampleSize(sampleSize).createSample()

    val sampleRDDRow = sampleRdd.asInstanceOf[RDD[Row]]
    val sampleDf = sc.createDataFrame(sampleRDDRow, df.schema)
    sampleDf

  }

}

object SamplerConverter {
  implicit def asDF(df : DataFrame)(implicit sc: SparkSession) =
    new DataFrameStatsFunctionsSampler(df)
}
