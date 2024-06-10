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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.Partition

import scala.collection.mutable

case class Sampler private(@transient private var sc: SparkContext = null, rdd: RDD[_] = null, sampleSize: Int = 0, funcKey: (Any) => String = { t => t.toString }) {
  def ofRDD(rdd: RDD[_]): Sampler = {
    copy(rdd = rdd)
  }

  def sparkContext(sc: SparkContext): Sampler = {
    copy(sc = sc)
  }

  def sampleSize(size: Int): Sampler = {
    copy(sampleSize = size)

  }

  def customKeys(f: (Any) => String): Sampler = {
    copy(funcKey = f)
  }

  def apply(): Sampler = {
    new Sampler()
  }

  private def calculateStatsOfRDD(rddforStats: RDD[_]) ={

    val rddInternal: RDD[_] = rddforStats
    val stats = rddInternal.mapPartitionsWithIndex((inx, rec) => {

      val statsCount = rec.foldLeft(mutable.Map[String, Int]())((accumulator: mutable.Map[String, Int], curr) => {
        val k = funcKey(curr)
        val v = accumulator.getOrElse(k, 0)

        accumulator.put(k, v + 1)
        accumulator
      })

      List((inx, statsCount)).iterator
    })

    stats.cache()
  }

  private def assignPartition( partitions : Array[Partition], partitionList: Array[Int]) :Array[Partition] ={

    var idx = 0
    val existPartitions = partitions.filter(t => partitionList.contains(t.index)).map(p => {
      val sp = new SubsetPartition(p, idx)
      idx = idx + 1
      val t: Partition = sp
      t
    })

    existPartitions
  }

  private def extractRequiredRecord(sampleRDD : RDD[_], partitionCombineStats: Array[(String, PartitionStats)], perKeySample: Double): RDD[_] ={

    val sampleRecordRDD = sampleRDD.mapPartitionsWithIndex((inx, r) => {
      var w = mutable.HashMap[String, Double]()

      partitionCombineStats.foreach(p => {

        val s = p._2.partition.size
        for ((tinx, v) <- p._2.partition.zipWithIndex) {

          if (v == inx) {
            if (tinx < s - 1) {
              w.put(p._1, 1.0)
            } else {
              w.put(p._1, p._2.lastWeight)
            }
          }
        }
        w
      })

      var keyCount: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()

      val sampleRecords = r.filter(rec => {
        val key = funcKey(rec)
        if (w.contains(key)) {
          keyCount.put(key, keyCount.getOrElse(key, 0) + 1)
        }

        w.contains(key) && keyCount(key) <= perKeySample * w(key)
      })

      sampleRecords.iterator
    })

    sampleRecordRDD
  }


  def createSample(): RDD[_] = {

    val stats = calculateStatsOfRDD(rdd)

    val keyList = stats.flatMap(t => t._2.keys).collect().distinct
    println(keyList.mkString(","))

    val perKeySample = sampleSize * 1.0 / keyList.size
    val ratioMap = stats.map(t => (t._1, t._2.map(r => (r._1, r._2 * 1.0 / perKeySample))))
    val sortedRatioMap = ratioMap.sortBy(f => -1 * f._2.size)

    val partitionStatsRDD = sortedRatioMap.flatMap(f => {
      f._2.map(k => (k._1, PartitionStats(k._1, List(f._1), k._2, k._2))).toList
    }).coalesce(sortedRatioMap.getNumPartitions / 10 + 1) //.fold((String, PartitionStats("0",List(),0.0))) (accumulateToMap)

    val partitionCombineStats = partitionStatsRDD.mapPartitions(m => {
      val accumulate = m.foldLeft(Map.empty[String, PartitionStats])(accumulateToMap)
      accumulate.iterator

    }).collect()

    val partitionList = partitionCombineStats.flatMap(t => t._2.partition).distinct

    val existPartitions = assignPartition(rdd.partitions, partitionList)

    println(existPartitions.map(p => p.index).toList)

    val sampleRDD = new SubsetRDD(sc, rdd.dependencies, rdd, existPartitions)
    val sampleRecordRDD = extractRequiredRecord(sampleRDD, partitionCombineStats, perKeySample)

    sampleRecordRDD.cache()
    sampleRecordRDD

  }

  private def accumulateToMap(accumulator: Map[String, PartitionStats], curr: (String, PartitionStats)): Map[String, PartitionStats] = {
    var tuple = ("key", PartitionStats("key", List(), 0.0, 0.0))
    if (accumulator.contains(curr._1)) {
      val a = accumulator(curr._1)

      val weight = curr._2.weight + a.weight
      val plist = a.partition ++ curr._2.partition

      var p = PartitionStats(curr._1, plist, weight, curr._2.weight)

      if (a.weight < 1.0) {
        if (weight > 1.0) {
          p = PartitionStats(curr._1, plist, weight, 1.0 - a.weight)
        } else {
          p = PartitionStats(curr._1, plist, weight, curr._2.weight)
        }
        tuple = (curr._1, p)
      }

    } else {

      var p = curr._2
      if (curr._2.weight > 1.0) {
        p = PartitionStats(curr._1, curr._2.partition, curr._2.weight, 1.0)
      } else {
        p = PartitionStats(curr._1, curr._2.partition, curr._2.weight, curr._2.weight)
      }
      tuple = (curr._1, p)
    }

    if (tuple._1.equals("key")) {
      accumulator
    } else {
      accumulator + tuple
    }
  }
}

private case class PartitionStats(key:String, partition:List[Int], weight: Double, lastWeight:Double)
