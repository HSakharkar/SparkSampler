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

import scala.reflect.ClassTag
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable

class SubsetRDD[T: ClassTag](@transient private var sc: SparkContext,
                             @transient private var deps: Seq[Dependency[_]],
                             rdd: RDD[T],
                             subsetPartitions: Array[Partition]
                            ) extends RDD[T](sc, Nil) {


  override def compute(split: Partition, context: TaskContext): Iterator[T] ={
    val spPartition:SubsetPartition = split.asInstanceOf[SubsetPartition]
    rdd.compute(spPartition.prev(), context)
  }

  override protected def getPartitions: Array[Partition] = subsetPartitions
}

private class SubsetPartition(prev:Partition, partitionId: Int) extends Partition {

  override def index: Int = {
    partitionId
  }

  def prev(): Partition ={
    return prev
  }

}
