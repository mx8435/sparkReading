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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.CompletionIterator

/**
 * 用于获取Shuffle的输出数据的位置信息（blockManager：FileSegment位置，FileSegment的大小）
 */
private[spark] class BlockStoreShuffleFetcher extends ShuffleFetcher with Logging {

  /**
   * 根据给定的ShuffleId和reducerId获取相应MapOutput数据的迭代器
   * @param shuffleId
   * @param reduceId
   * @param context
   * @param serializer
   * @tparam T
   * @return An iterator over the elements of the fetched shuffle outputs.
   */
  override def fetch[T](
      shuffleId: Int,
      reduceId: Int,
      context: TaskContext,
      serializer: Serializer)
    : Iterator[T] =
  {

    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val blockManager = SparkEnv.get.blockManager

    val startTime = System.currentTimeMillis
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)//获取MapStatuses
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))

    //将属于同一个地址blockManager的所有FileSegments信息聚集在一起，即splitsByAddress中存放了以地址为key，
    // 该地址中所有FileSegments信息为value的HashMap
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }

    //创建对应的blocksByAddress
    val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address,
          splits.map(s => (ShuffleBlockId(shuffleId, s._1, reduceId), s._2))//对每个FileSegment信息，封装成一个ShuffleBlockId
        )
    }

    def unpackBlock(blockPair: (BlockId, Option[Iterator[Any]])) : Iterator[T] = {
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Some(block) => {
          block.asInstanceOf[Iterator[T]]
        }
        case None => {
          blockId match {
            case ShuffleBlockId(shufId, mapId, _) =>
              val address = statuses(mapId.toInt)._1
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, null)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block")
          }
        }
      }
    }

    val blockFetcherItr = blockManager.getMultiple(blocksByAddress, serializer)//根据封装好的blocksByAddress，统计需要远程获取的文件块，将这种请求加入到blockFetcherItr中的请求队列，
    // 让BlockFetcherItr去获取相应的数据
    val itr = blockFetcherItr.flatMap(unpackBlock)

    val completionIter = CompletionIterator[T, Iterator[T]](itr, {
      val shuffleMetrics = new ShuffleReadMetrics
      shuffleMetrics.shuffleFinishTime = System.currentTimeMillis
      shuffleMetrics.fetchWaitTime = blockFetcherItr.fetchWaitTime
      shuffleMetrics.remoteBytesRead = blockFetcherItr.remoteBytesRead
      shuffleMetrics.totalBlocksFetched = blockFetcherItr.totalBlocks
      shuffleMetrics.localBlocksFetched = blockFetcherItr.numLocalBlocks
      shuffleMetrics.remoteBlocksFetched = blockFetcherItr.numRemoteBlocks
      context.taskMetrics.shuffleReadMetrics = Some(shuffleMetrics)
    })

    new InterruptibleIterator[T](context, completionIter)
  }
}
