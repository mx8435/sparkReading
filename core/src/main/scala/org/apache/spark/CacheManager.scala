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

package org.apache.spark

import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, BlockManager, BlockStatus, RDDBlockId, StorageLevel}

/**负责传递rdd的分区给blockManager
 * Spark class responsible for passing RDDs split contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
 */
private[spark] class CacheManager(blockManager: BlockManager) extends Logging {

  /** Keys of RDD splits that are being computed/loaded. */
  private val loading = new HashSet[RDDBlockId]()

  /** 当一个RDD cache时，会调用该方法获取或计算该RDD的split
   * Gets or computes an RDD split. Used by RDD.iterator() when an RDD is cached. */
  def getOrCompute[T](rdd: RDD[T], split: Partition, context: TaskContext,
      storageLevel: StorageLevel): Iterator[T] = {
    val key = RDDBlockId(rdd.id, split.index)//标识该split
    logDebug("Looking for partition " + key)
    blockManager.get(key) match {//判断该RDD的split是否已经缓存在blockManager
      case Some(values) =>
        // Partition is already materialized, so just return its values
        new InterruptibleIterator(context, values.asInstanceOf[Iterator[T]])//已经存在blockManager中了

      case None =>
        //不在blockManager中
        // Mark the split as loading (unless someone else marks it first)
        loading.synchronized {
          if (loading.contains(key)) {//如果已经有将该split标记为loading了
            logInfo("Another thread is loading %s, waiting for it to finish...".format(key))
            while (loading.contains(key)) {
              try {
                loading.wait()//等待载入该split
              } catch {
                case e: Exception =>
                  logWarning(s"Got an exception while waiting for another thread to load $key", e)
              }
            }
            logInfo("Finished waiting for %s".format(key))
            /* See whether someone else has successfully loaded it. The main way this would fail
             * is for the RDD-level cache eviction policy if someone else has loaded the same RDD
             * partition but we didn't want to make space for it. However, that case is unlikely
             * because it's unlikely that two threads would work on the same RDD partition. One
             * downside of the current code is that threads wait serially if this does happen. */
            //已经成功将该split加载到内存blockManager中，只需再一次次从blockManager中提取该值
             blockManager.get(key) match {
              case Some(values) =>
                return new InterruptibleIterator(context, values.asInstanceOf[Iterator[T]])
              case None =>
                logInfo("Whoever was loading %s failed; we'll try it ourselves".format(key))
                loading.add(key)
            }
          } else {
            loading.add(key)//没有将该split标记为loading过，需要标记。然后在接下来进行加载
          }
        }
        try {
          // If we got here, we have to load the split
          logInfo("Partition %s not found, computing it".format(key))
          val computedValues = rdd.computeOrReadCheckpoint(split, context)//获取该分区对应的记录

          // Persist the result, so long as the task is not running locally
          if (context.runningLocally) {
            return computedValues
          }

          // Keep track of blocks with updated statuses
          var updatedBlocks = Seq[(BlockId, BlockStatus)]()
          val returnValue: Iterator[T] = {
            if (storageLevel.useDisk && !storageLevel.useMemory) {//只采用存储到磁盘
              /* In the case that this RDD is to be persisted using DISK_ONLY
               * the iterator will be passed directly to the blockManager (rather then
               * caching it to an ArrayBuffer first), then the resulting block data iterator
               * will be passed back to the user. If the iterator generates a lot of data,
               * this means that it doesn't all have to be held in memory at one time.
               * This could also apply to MEMORY_ONLY_SER storage, but we need to make sure
               * blocks aren't dropped by the block store before enabling that. */
              updatedBlocks = blockManager.put(key, computedValues, storageLevel, tellMaster = true)
              blockManager.get(key) match {
                case Some(values) =>
                  values.asInstanceOf[Iterator[T]]
                case None =>
                  logInfo("Failure to store %s".format(key))
                  throw new Exception("Block manager failed to return persisted valued")
              }
            } else {//需要将该数据cache到blockManager中：将记录存到一个ArrayBuffer中，然后将这个ArrayBuffer插入到blockManager中
              // In this case the RDD is cached to an array buffer. This will save the results
              // if we're dealing with a 'one-time' iterator
              val elements = new ArrayBuffer[Any]
              elements ++= computedValues
              updatedBlocks = blockManager.put(key, elements, storageLevel, tellMaster = true)
              elements.iterator.asInstanceOf[Iterator[T]]//返回对应的迭代器
            }
          }

          // Update task metrics to include any blocks whose storage status is updated
          val metrics = context.taskMetrics
          metrics.updatedBlocks = Some(updatedBlocks)

          new InterruptibleIterator(context, returnValue)

        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }
}
