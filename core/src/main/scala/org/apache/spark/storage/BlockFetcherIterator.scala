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

package org.apache.spark.storage

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.Queue

import io.netty.buffer.ByteBuf

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.network.BufferMessage
import org.apache.spark.network.ConnectionManagerId
import org.apache.spark.network.netty.ShuffleCopier
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils

/**
 * A block fetcher iterator interface. There are two implementations:
 *
 * BasicBlockFetcherIterator: uses a custom-built NIO communication layer.
 * NettyBlockFetcherIterator: uses Netty (OIO) as the communication layer.
 *
 * Eventually we would like the two to converge and use a single NIO-based communication layer,
 * but extensive tests show that under some circumstances (e.g. large shuffles with lots of cores),
 * NIO would perform poorly and thus the need for the Netty OIO one.
 */

private[storage]
trait BlockFetcherIterator extends Iterator[(BlockId, Option[Iterator[Any]])] with Logging {
  def initialize()
  def totalBlocks: Int
  def numLocalBlocks: Int
  def numRemoteBlocks: Int
  def fetchWaitTime: Long
  def remoteBytesRead: Long
}


/**
 * 用于获取各个MapOutput的文件块FileSegment(在ShuffleDep中)
 */
private[storage]
object BlockFetcherIterator {

  // A request to fetch one or more blocks, complete with their sizes
  class FetchRequest(val address: BlockManagerId, val blocks: Seq[(BlockId, Long)]) {
    val size = blocks.map(_._2).sum
  }

  // A result of a fetch. Includes the block ID, size in bytes, and a function to deserialize
  // the block (since we want all deserializaton to happen in the calling thread); can also
  // represent a fetch failure if size == -1.
  class FetchResult(val blockId: BlockId, val size: Long, val deserialize: () => Iterator[Any]) {
    def failed: Boolean = size == -1
  }

  class BasicBlockFetcherIterator(
      private val blockManager: BlockManager,
      val blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
      serializer: Serializer)
    extends BlockFetcherIterator {

    import blockManager._

    private var _remoteBytesRead = 0L
    private var _fetchWaitTime = 0L

    if (blocksByAddress == null) {
      throw new IllegalArgumentException("BlocksByAddress is null")
    }

    // Total number blocks fetched (local + remote). Also number of FetchResults expected
    protected var _numBlocksToFetch = 0

    protected var startTime = System.currentTimeMillis

    // This represents the number of local blocks, also counting zero-sized blocks
    private var numLocal = 0
    // BlockIds for local blocks that need to be fetched. Excludes zero-sized blocks
    protected val localBlocksToFetch = new ArrayBuffer[BlockId]()

    // This represents the number of remote blocks, also counting zero-sized blocks
    private var numRemote = 0
    // BlockIds for remote blocks that need to be fetched. Excludes zero-sized blocks
    protected val remoteBlocksToFetch = new HashSet[BlockId]()

    // A queue to hold our results.
    protected val results = new LinkedBlockingQueue[FetchResult]

    // Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
    // the number of bytes in flight is limited to maxBytesInFlight
    private val fetchRequests = new Queue[FetchRequest]

    // Current bytes in flight from our requests
    private var bytesInFlight = 0L

    /**
     * 根据传过来的FetchRequest去获取数据
     * @param req
     */
    protected def sendRequest(req: FetchRequest) {
      logDebug("Sending request for %d blocks (%s) from %s".format(
        req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
      val cmId = new ConnectionManagerId(req.address.host, req.address.port)
      val blockMessageArray = new BlockMessageArray(req.blocks.map {
        case (blockId, size) => BlockMessage.fromGetBlock(GetBlock(blockId))
      })
      bytesInFlight += req.size
      val sizeMap = req.blocks.toMap  // so we can look up the size of each blockID
      val future = connectionManager.sendMessageReliably(cmId, blockMessageArray.toBufferMessage)//以可靠的方式发送blockMessageArray
      future.onSuccess {
        case Some(message) => {
          val bufferMessage = message.asInstanceOf[BufferMessage]
          val blockMessageArray = BlockMessageArray.fromBufferMessage(bufferMessage)
          for (blockMessage <- blockMessageArray) {
            if (blockMessage.getType != BlockMessage.TYPE_GOT_BLOCK) {
              throw new SparkException(
                "Unexpected message " + blockMessage.getType + " received from " + cmId)
            }
            val blockId = blockMessage.getId
            val networkSize = blockMessage.getData.limit()
            results.put(new FetchResult(blockId, sizeMap(blockId),
              () => dataDeserialize(blockId, blockMessage.getData, serializer)))//根据返回的块信息生成一个FetchResult
            _remoteBytesRead += networkSize
            logDebug("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
          }
        }
        case None => {
          logError("Could not get block(s) from " + cmId)
          for ((blockId, size) <- req.blocks) {
            results.put(new FetchResult(blockId, -1, null))
          }
        }
      }
    }

    /**
     * 根据每个块位置信息，区分出本地块请求和远程块请求，并将其分别加入到localBlocksToFetch和remoteBlocksToFetch中。
     * 对于远程块请求，由于一个FetchRequest可请求的数据有限，因此在一个FetchRequest容量够了时创建一个新的FetchRequest。
     * 注意一个FetchRequest可以负责多个块：curBlocks += ((blockId, size))//该FetchRequest需负责的文件块
     * @return
     */
    protected def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
      // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
      // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
      // nodes, rather than blocking on reading output from one node.
      val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)//受内存缓存的限制，应尽量使每个fetchRequest请求的数据大小不超过此值
      logInfo("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

      // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
      // at most maxBytesInFlight in order to limit the amount of data in flight.
      val remoteRequests = new ArrayBuffer[FetchRequest]
      for ((address, blockInfos) <- blocksByAddress) {
        if (address == blockManagerId) {//该文件块在本地
          numLocal = blockInfos.size//本地有的文件块数
          // Filter out zero-sized blocks
          localBlocksToFetch ++= blockInfos.filter(_._2 != 0).map(_._1)
          _numBlocksToFetch += localBlocksToFetch.size//更新获取的文件块信息
        } else {//不在本地
          numRemote += blockInfos.size//该地址对应的块数
          val iterator = blockInfos.iterator
          var curRequestSize = 0L
          var curBlocks = new ArrayBuffer[(BlockId, Long)]
          while (iterator.hasNext) {
            val (blockId, size) = iterator.next()
            // Skip empty blocks
            if (size > 0) {
              curBlocks += ((blockId, size))//该FetchRequest需负责的文件块
              remoteBlocksToFetch += blockId//入队
              _numBlocksToFetch += 1
              curRequestSize += size//更新需远程获取的总数据
            } else if (size < 0) {
              throw new BlockException(blockId, "Negative block size " + size)
            }
            if (curRequestSize >= targetRequestSize) {//由于一个FetchRequest一次可请求的数据有限，因此，当该FetchRequest的需获取的数据够了时，重新创建一个FetchRequest
              // Add this FetchRequest
              remoteRequests += new FetchRequest(address, curBlocks)
              curRequestSize = 0
              curBlocks = new ArrayBuffer[(BlockId, Long)]
              logDebug(s"Creating fetch request of $curRequestSize at $address")
            }
          }
          // Add in the final request
          if (!curBlocks.isEmpty) {
            remoteRequests += new FetchRequest(address, curBlocks)
          }
        }
      }
      logInfo("Getting " + _numBlocksToFetch + " non-empty blocks out of " +
        totalBlocks + " blocks")
      remoteRequests
    }

    protected def getLocalBlocks() {
      // Get the local blocks while remote blocks are being fetched. Note that it's okay to do
      // these all at once because they will just memory-map some files, so they won't consume
      // any memory that might exceed our maxBytesInFlight
      for (id <- localBlocksToFetch) {
        getLocalFromDisk(id, serializer) match {
          case Some(iter) => {
            // Pass 0 as size since it's not in flight
            results.put(new FetchResult(id, 0, () => iter))
            logDebug("Got local block " + id)
          }
          case None => {
            throw new BlockException(id, "Could not get block " + id + " from local machine")
          }
        }
      }
    }

    /**
     * blockFetcher的初始化：统计需要远程获取的数据块，随机方式加入到fetchRequests请求队列中，获取本地的块
     */
    override def initialize() {
      // Split local and remote blocks.
      val remoteRequests = splitLocalRemoteBlocks()
      // Add the remote requests into our queue in a random order
      fetchRequests ++= Utils.randomize(remoteRequests)//随机方式加入到fetchRequests请求队列中

      // Send out initial requests for blocks, up to our maxBytesInFlight
      //
      while (!fetchRequests.isEmpty &&
        (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
        sendRequest(fetchRequests.dequeue())
      }

      val numFetches = remoteRequests.size - fetchRequests.size//尚且需要开启的fetchRequest数目，fetchRequests.size是当前已有的
      logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))//其实没有再开启numFetches个request

      // Get Local Blocks
      startTime = System.currentTimeMillis
      getLocalBlocks()//获取本地的块
      logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime) + " ms")
    }

    override def totalBlocks: Int = numLocal + numRemote
    override def numLocalBlocks: Int = numLocal
    override def numRemoteBlocks: Int = numRemote
    override def fetchWaitTime: Long = _fetchWaitTime
    override def remoteBytesRead: Long = _remoteBytesRead


    // Implementing the Iterator methods with an iterator that reads fetched blocks off the queue
    // as they arrive.
    @volatile protected var resultsGotten = 0

    override def hasNext: Boolean = resultsGotten < _numBlocksToFetch

    /**
     * 返回(块数据对应的块信息blockId,经过反序列化的块数据)
     * @return
     */
    override def next(): (BlockId, Option[Iterator[Any]]) = {
      resultsGotten += 1
      val startFetchWait = System.currentTimeMillis()
      val result = results.take()//获取下一个MapOutput的结果(FileSegment数据)
      val stopFetchWait = System.currentTimeMillis()
      _fetchWaitTime += (stopFetchWait - startFetchWait)
      if (! result.failed) bytesInFlight -= result.size//更新在内存中的数据，即一个BasicBlockFetcherIterator同时存到内存的数据大小
      while (!fetchRequests.isEmpty &&
        (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {//BasicBlockFetcherIterator会充分利用可用的内存空间，当尚且可以存bytesInFlight时，会再发送fetchRequest获取数据
        sendRequest(fetchRequests.dequeue())
      }
      (result.blockId, if (result.failed) None else Some(result.deserialize()))//获取数据
    }
  }
  // End of BasicBlockFetcherIterator

  class NettyBlockFetcherIterator(
      blockManager: BlockManager,
      blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
      serializer: Serializer)
    extends BasicBlockFetcherIterator(blockManager, blocksByAddress, serializer) {

    import blockManager._

    val fetchRequestsSync = new LinkedBlockingQueue[FetchRequest]

    private def startCopiers(numCopiers: Int): List[_ <: Thread] = {
      (for ( i <- Range(0,numCopiers) ) yield {
        val copier = new Thread {
          override def run(){
            try {
              while(!isInterrupted && !fetchRequestsSync.isEmpty) {
                sendRequest(fetchRequestsSync.take())
              }
            } catch {
              case x: InterruptedException => logInfo("Copier Interrupted")
              // case _ => throw new SparkException("Exception Throw in Shuffle Copier")
            }
          }
        }
        copier.start
        copier
      }).toList
    }

    // keep this to interrupt the threads when necessary
    private def stopCopiers() {
      for (copier <- copiers) {
        copier.interrupt()
      }
    }

    override protected def sendRequest(req: FetchRequest) {

      def putResult(blockId: BlockId, blockSize: Long, blockData: ByteBuf) {
        val fetchResult = new FetchResult(blockId, blockSize,
          () => dataDeserialize(blockId, blockData.nioBuffer, serializer))
        results.put(fetchResult)
      }

      logDebug("Sending request for %d blocks (%s) from %s".format(
        req.blocks.size, Utils.bytesToString(req.size), req.address.host))
      val cmId = new ConnectionManagerId(req.address.host, req.address.nettyPort)
      val cpier = new ShuffleCopier(blockManager.conf)
      cpier.getBlocks(cmId, req.blocks, putResult)
      logDebug("Sent request for remote blocks " + req.blocks + " from " + req.address.host )
    }

    private var copiers: List[_ <: Thread] = null

    override def initialize() {
      // Split Local Remote Blocks and set numBlocksToFetch
      val remoteRequests = splitLocalRemoteBlocks()
      // Add the remote requests into our queue in a random order
      for (request <- Utils.randomize(remoteRequests)) {
        fetchRequestsSync.put(request)
      }

      copiers = startCopiers(conf.getInt("spark.shuffle.copier.threads", 6))
      logInfo("Started " + fetchRequestsSync.size + " remote fetches in " +
        Utils.getUsedTimeMs(startTime))

      // Get Local Blocks
      startTime = System.currentTimeMillis
      getLocalBlocks()
      logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime) + " ms")
    }

    override def next(): (BlockId, Option[Iterator[Any]]) = {
      resultsGotten += 1
      val result = results.take()
      // If all the results has been retrieved, copiers will exit automatically
      (result.blockId, if (result.failed) None else Some(result.deserialize()))
    }
  }
  // End of NettyBlockFetcherIterator
}
