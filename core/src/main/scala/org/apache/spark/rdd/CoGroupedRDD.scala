/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark.{InterruptibleIterator, Partition, Partitioner, SparkEnv, TaskContext}
import org.apache.spark.{Dependency, OneToOneDependency, ShuffleDependency}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.{ExternalAppendOnlyMap, AppendOnlyMap}
import org.apache.spark.serializer.Serializer

private[spark] sealed trait CoGroupSplitDep extends Serializable

private[spark] case class NarrowCoGroupSplitDep(
    rdd: RDD[_],
    splitIndex: Int,
    var split: Partition
  ) extends CoGroupSplitDep {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

private[spark] case class ShuffleCoGroupSplitDep(shuffleId: Int) extends CoGroupSplitDep

private[spark] class CoGroupPartition(idx: Int, val deps: Array[CoGroupSplitDep])
  extends Partition with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

/**
 * :: DeveloperApi ::
 * A RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * Note: This is an internal API. We recommend users use RDD.coGroup(...) instead of
 * instantiating this directly.

 * @param rdds parent RDDs.
 * @param part partitioner used to partition the shuffle output
 */
@DeveloperApi
class CoGroupedRDD[K](@transient var rdds: Seq[RDD[_ <: Product2[K, _]]], part: Partitioner)
  extends RDD[(K, Seq[Seq[_]])](rdds.head.context, Nil) {

  // For example, `(k, a) cogroup (k, b)` produces k -> Seq(ArrayBuffer as, ArrayBuffer bs).
  // Each ArrayBuffer is represented as a CoGroup, and the resulting Seq as a CoGroupCombiner.
  // CoGroupValue is the intermediate state of each value before being merged in compute.
  private type CoGroup = ArrayBuffer[Any]
  private type CoGroupValue = (Any, Int)  // Int is dependency number
  private type CoGroupCombiner = Seq[CoGroup]

  private var serializer: Serializer = null

  def setSerializer(serializer: Serializer): CoGroupedRDD[K] = {
    this.serializer = serializer
    this
  }

  /**
   * 根据各父RDD与子RDD的partitioner是否一致，返回父子RDD的依赖关系。
   * 仅当二者的partitioner和numPartitions均一致时才为OneToOneDependency，否侧为ShuffleDependency
   * @return
   */
  override def getDependencies: Seq[Dependency[_]] = {
    //采用Seq.map依次生成各依赖关系
    rdds.map { rdd: RDD[_ <: Product2[K, _]] =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)//如果有partitioner是相同的，则是1:1依赖
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[Any, Any](rdd, part, serializer)//否则为ShuffleDependency
      }
    }
  }

  /**
   * 获取当前CoGroupedRDD的各个分区，保存在Array中，数组的大小有partitioner中的分区数指定
   * @return
   */
  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)//CoGroupedRDD的partitions数目
    for (i <- 0 until array.size) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId)
          case _ =>
            new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i))
        }
      }.toArray)
    }
    array
  }

  override val partitioner: Some[Partitioner] = Some(part)

  override def compute(s: Partition, context: TaskContext): Iterator[(K, CoGroupCombiner)] = {
    val sparkConf = SparkEnv.get.conf
    val externalSorting = sparkConf.getBoolean("spark.shuffle.spill", true)
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = split.deps.size

    // A list of (rdd iterator, dependency number) pairs
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) =>
        // Read them from the parent
        val it = rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, Any]]]
        rddIterators += ((it, depNum))

      case ShuffleCoGroupSplitDep(shuffleId) =>
        // Read map outputs of shuffle
        val fetcher = SparkEnv.get.shuffleFetcher
        val ser = Serializer.getSerializer(serializer)
        val it = fetcher.fetch[Product2[K, Any]](shuffleId, split.index, context, ser)
        rddIterators += ((it, depNum))//获取所有Shuffle输出对应的Iterator和依赖号dep Num
    }

    if (!externalSorting) {//全在内存中进行，采用AppendOnlyMap
      val map = new AppendOnlyMap[K, CoGroupCombiner]
      val update: (Boolean, CoGroupCombiner) => CoGroupCombiner = (hadVal, oldVal) => {
        if (hadVal) oldVal else Array.fill(numRdds)(new CoGroup)//如果已经存在该值，就将返回oldValues，否则新生成
      }
      val getCombiner: K => CoGroupCombiner = key => {
        map.changeValue(key, update)
      }
      rddIterators.foreach { case (it, depNum) =>
        while (it.hasNext) {
          val kv = it.next()
          getCombiner(kv._1)(depNum) += kv._2
        }
      }
      new InterruptibleIterator(context, map.iterator)
    } else {
      val map = createExternalMap(numRdds)
      rddIterators.foreach { case (it, depNum) =>
        while (it.hasNext) {
          val kv = it.next()
          map.insert(kv._1, new CoGroupValue(kv._2, depNum))
        }
      }
      context.taskMetrics.memoryBytesSpilled = map.memoryBytesSpilled
      context.taskMetrics.diskBytesSpilled = map.diskBytesSpilled
      new InterruptibleIterator(context, map.iterator)
    }
  }

  private def createExternalMap(numRdds: Int)
    : ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner] = {

    val createCombiner: (CoGroupValue => CoGroupCombiner) = value => {
      val newCombiner = Array.fill(numRdds)(new CoGroup)
      value match { case (v, depNum) => newCombiner(depNum) += v }
      newCombiner
    }
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
      value match { case (v, depNum) => combiner(depNum) += v }
      combiner
    }
    val mergeCombiners: (CoGroupCombiner, CoGroupCombiner) => CoGroupCombiner =
      (combiner1, combiner2) => {
        combiner1.zip(combiner2).map { case (v1, v2) => v1 ++ v2 }
      }
    new ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner](
      createCombiner, mergeValue, mergeCombiners)
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
