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

package org.apache.spark.broadcast

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}

import scala.reflect.ClassTag
import scala.math
import scala.util.Random

import org.apache.spark.{Logging, SparkConf, SparkEnv, SparkException}
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util.Utils

/**p2p
 *  A [[org.apache.spark.broadcast.Broadcast]] implementation that uses a BitTorrent-like
 *  protocol to do a distributed transfer of the broadcasted data to the executors.
 *  The mechanism is as follows. The driver divides the serializes the broadcasted data,
 *  divides it into smaller chunks, and stores them in the BlockManager of the driver.
 *  These chunks are reported to the BlockManagerMaster so that all the executors can
 *  learn the location of those chunks. The first time the broadcast variable (sent as
 *  part of task) is deserialized at a executor, all the chunks are fetched using
 *  the BlockManager. When all the chunks are fetched (initially from the driver's
 *  BlockManager), they are combined and deserialized to recreate the broadcasted data.
 *  However, the chunks are also stored in the BlockManager and reported to the
 *  BlockManagerMaster. As more executors fetch the chunks, BlockManagerMaster learns
 *  multiple locations for each chunk. Hence, subsequent fetches of each chunk will be
 *  made to other executors who already have those chunks, resulting in a distributed
 *  fetching. This prevents the driver from being the bottleneck in sending out multiple
 *  copies of the broadcast data (one per executor) as done by the
 *  [[org.apache.spark.broadcast.HttpBroadcast]].
 */
private[spark] class TorrentBroadcast[T: ClassTag](
    @transient var value_ : T, isLocal: Boolean, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  def getValue = value_

  val broadcastId = BroadcastBlockId(id)

  TorrentBroadcast.synchronized {
    SparkEnv.get.blockManager.putSingle(//会先存放到blockManager中
      broadcastId, value_, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
  }

  @transient var arrayOfBlocks: Array[TorrentBlock] = null
  @transient var totalBlocks = -1
  @transient var totalBytes = -1
  @transient var hasBlocks = 0

  if (!isLocal) {//不是本地模式的话
    sendBroadcast()
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  def doUnpersist(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  def doDestroy(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /**对广播变量进行分块；
   * 将bcdata对应的元数据和各数据分块依次存入blockManager
   */
  def sendBroadcast() {
    val tInfo = TorrentBroadcast.blockifyObject(value_)//分块
    totalBlocks = tInfo.totalBlocks
    totalBytes = tInfo.totalBytes
    hasBlocks = tInfo.totalBlocks

    // Store meta-info存储元数据至blockManager
    val metaId = BroadcastBlockId(id, "meta")
    val metaInfo = TorrentInfo(null, totalBlocks, totalBytes)
    TorrentBroadcast.synchronized {
      SparkEnv.get.blockManager.putSingle(
        metaId, metaInfo, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    }

    // Store individual pieces
    for (i <- 0 until totalBlocks) {//依次存入各个dataBlocks到blockManager
      val pieceId = BroadcastBlockId(id, "piece" + i)
      TorrentBroadcast.synchronized {
        SparkEnv.get.blockManager.putSingle(
          pieceId, tInfo.arrayOfBlocks(i), StorageLevel.MEMORY_AND_DISK, tellMaster = true)//通知blockManagerMaster我数据已经存好
      }
    }
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream) {
    assertValid()
    out.defaultWriteObject()
  }

  /** 在JVM反序列化task中的广播变量时调用
   * Used by the JVM when deserializing this object. */
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    TorrentBroadcast.synchronized {
      SparkEnv.get.blockManager.getSingle(broadcastId) match {
        case Some(x) =>
          value_ = x.asInstanceOf[T]//命中，直接返回

        case None =>
          //不命中，则需去读取
          val start = System.nanoTime
          logInfo("Started reading broadcast variable " + id)

          // Initialize @transient variables that will receive garbage values from the master.
          resetWorkerVariables()

          if (receiveBroadcast()) {//需去读取
            value_ = TorrentBroadcast.unBlockifyObject[T](arrayOfBlocks, totalBytes, totalBlocks)

            /* Store the merged copy in cache so that the next worker doesn't need to rebuild it.
             * This creates a trade-off between memory usage and latency. Storing copy doubles
             * the memory footprint; not storing doubles deserialization cost. Also,
             * this does not need to be reported to BlockManagerMaster since other executors
             * does not need to access this block (they only need to fetch the chunks,
             * which are reported).
             */
            SparkEnv.get.blockManager.putSingle(
              broadcastId, value_, StorageLevel.MEMORY_AND_DISK, tellMaster = false)

            // Remove arrayOfBlocks from memory once value_ is on local cache
            resetWorkerVariables()
          } else {
            logError("Reading broadcast variable " + id + " failed")
          }

          val time = (System.nanoTime - start) / 1e9
          logInfo("Reading broadcast variable " + id + " took " + time + " s")
      }
    }
  }

  private def resetWorkerVariables() {
    arrayOfBlocks = null
    totalBytes = -1
    totalBlocks = -1
    hasBlocks = 0
  }

  /**
   *
   * 获取广播变量：先读取该broadcast变量的元数据，
   * @return
   */
  def receiveBroadcast(): Boolean = {
    // Receive meta-info about the size of broadcast data,
    // the number of chunks it is divided into, etc.
    val metaId = BroadcastBlockId(id, "meta")
    var attemptId = 10
    while (attemptId > 0 && totalBlocks == -1) {//失败重试
      TorrentBroadcast.synchronized {
        SparkEnv.get.blockManager.getSingle(metaId) match {//先读取元数据
          case Some(x) =>
            val tInfo = x.asInstanceOf[TorrentInfo]
            totalBlocks = tInfo.totalBlocks
            totalBytes = tInfo.totalBytes
            arrayOfBlocks = new Array[TorrentBlock](totalBlocks)
            hasBlocks = 0

          case None =>
            Thread.sleep(500)//为何要休眠？因为同步？
        }
      }
      attemptId -= 1
    }
    if (totalBlocks == -1) {
      return false
    }

    /*读取真正的数据块（chunk）
     * Fetch actual chunks of data. Note that all these chunks are stored in
     * the BlockManager and reported to the master, so that other executors
     * can find out and pull the chunks from this executor.
     */
    val recvOrder = new Random().shuffle(Array.iterate(0, totalBlocks)(_ + 1).toList)//打乱顺序去请求
    for (pid <- recvOrder) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      TorrentBroadcast.synchronized {
        SparkEnv.get.blockManager.getSingle(pieceId) match {
          case Some(x) =>
            arrayOfBlocks(pid) = x.asInstanceOf[TorrentBlock]
            hasBlocks += 1
            SparkEnv.get.blockManager.putSingle(
              pieceId, arrayOfBlocks(pid), StorageLevel.MEMORY_AND_DISK, tellMaster = true)

          case None =>
            throw new SparkException("Failed to get " + pieceId + " of " + broadcastId)
        }
      }
    }

    hasBlocks == totalBlocks
  }

}

/**
 * TorrentBroadcast的伴生对象
 */
private[spark] object TorrentBroadcast extends Logging {
  private lazy val BLOCK_SIZE = conf.getInt("spark.broadcast.blockSize", 4096) * 1024
  private var initialized = false
  private var conf: SparkConf = null

  /**
   * 初始化，没做什么
   * @param _isDriver
   * @param conf
   */
  def initialize(_isDriver: Boolean, conf: SparkConf) {
    TorrentBroadcast.conf = conf // TODO: we might have to fix it in tests
    synchronized {
      if (!initialized) {
        initialized = true
      }
    }
  }

  def stop() {
    initialized = false
  }

  /**
   * 对广播变量进行分块
   * @param obj
   * @tparam T
   * @return
   */
  def blockifyObject[T](obj: T): TorrentInfo = {
    val byteArray = Utils.serialize[T](obj)//先序列化
    val bais = new ByteArrayInputStream(byteArray)

    var blockNum = byteArray.length / BLOCK_SIZE //块数目，一个块默认大小为4M
    if (byteArray.length % BLOCK_SIZE != 0) {//补齐
      blockNum += 1
    }

    val blocks = new Array[TorrentBlock](blockNum)//存放对应的所有块
    var blockId = 0

    for (i <- 0 until (byteArray.length, BLOCK_SIZE)) {//对每个块
      val thisBlockSize = math.min(BLOCK_SIZE, byteArray.length - i)//该块的大小
      val tempByteArray = new Array[Byte](thisBlockSize)
      bais.read(tempByteArray, 0, thisBlockSize)

      blocks(blockId) = new TorrentBlock(blockId, tempByteArray)//按顺序编号，并切分相应大小的数据块
      blockId += 1
    }
    bais.close()//关闭流

    val info = TorrentInfo(blocks, blockNum, byteArray.length)//封装
    info.hasBlocks = blockNum
    info
  }

  /**
   * 对分块进行合并组装并反序列化
   * @param arrayOfBlocks
   * @param totalBytes
   * @param totalBlocks
   * @tparam T
   * @return
   */
  def unBlockifyObject[T](
      arrayOfBlocks: Array[TorrentBlock],
      totalBytes: Int,
      totalBlocks: Int): T = {
    val retByteArray = new Array[Byte](totalBytes)
    for (i <- 0 until totalBlocks) {
      System.arraycopy(arrayOfBlocks(i).byteArray, 0, retByteArray,
        i * BLOCK_SIZE, arrayOfBlocks(i).byteArray.length)
    }
    Utils.deserialize[T](retByteArray, Thread.currentThread.getContextClassLoader)
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean) = synchronized {
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}

private[spark] case class TorrentBlock(
    blockID: Int,
    byteArray: Array[Byte])
  extends Serializable

private[spark] case class TorrentInfo(
    @transient arrayOfBlocks: Array[TorrentBlock],
    totalBlocks: Int,
    totalBytes: Int)
  extends Serializable {

  @transient var hasBlocks = 0
}
