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

package org.apache.spark.scheduler

import scala.language.existentials

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.rdd.{RDD, RDDCheckpointData}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._

private[spark] object ShuffleMapTask {

  // A simple map between the stage id to the serialized byte array of a task.
  // Served as a cache for task serialization because serialization can be
  // expensive on the master node if it needs to launch thousands of tasks.
  private val serializedInfoCache = new HashMap[Int, Array[Byte]]

  def serializeInfo(stageId: Int, rdd: RDD[_], dep: ShuffleDependency[_,_]): Array[Byte] = {
    synchronized {
      val old = serializedInfoCache.get(stageId).orNull
      if (old != null) {
        return old
      } else {
        val out = new ByteArrayOutputStream
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val objOut = ser.serializeStream(new GZIPOutputStream(out))
        objOut.writeObject(rdd)
        objOut.writeObject(dep)
        objOut.close()
        val bytes = out.toByteArray
        serializedInfoCache.put(stageId, bytes)
        bytes
      }
    }
  }

  def deserializeInfo(stageId: Int, bytes: Array[Byte]): (RDD[_], ShuffleDependency[_,_]) = {
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val objIn = ser.deserializeStream(in)
    val rdd = objIn.readObject().asInstanceOf[RDD[_]]
    val dep = objIn.readObject().asInstanceOf[ShuffleDependency[_,_]]
    (rdd, dep)
  }

  // Since both the JarSet and FileSet have the same format this is used for both.
  def deserializeFileSet(bytes: Array[Byte]): HashMap[String, Long] = {
    val in = new GZIPInputStream(new ByteArrayInputStream(bytes))
    val objIn = new ObjectInputStream(in)
    val set = objIn.readObject().asInstanceOf[Array[(String, Long)]].toMap
    HashMap(set.toSeq: _*)
  }

  def removeStage(stageId: Int) {
    serializedInfoCache.remove(stageId)
  }

  def clearCache() {
    synchronized {
      serializedInfoCache.clear()
    }
  }
}

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param rdd the final RDD in this stage
 * @param dep the ShuffleDependency
 * @param _partitionId index of the number in the RDD
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    var rdd: RDD[_],
    var dep: ShuffleDependency[_,_],
    _partitionId: Int,
    @transient private var locs: Seq[TaskLocation])
  extends Task[MapStatus](stageId, _partitionId)
  with Externalizable
  with Logging {

  protected def this() = this(0, null, null, 0, null)

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  var split = if (rdd == null) null else rdd.partitions(partitionId)

  override def writeExternal(out: ObjectOutput) {
    RDDCheckpointData.synchronized {
      split = rdd.partitions(partitionId)
      out.writeInt(stageId)
      val bytes = ShuffleMapTask.serializeInfo(stageId, rdd, dep)
      out.writeInt(bytes.length)
      out.write(bytes)
      out.writeInt(partitionId)
      out.writeLong(epoch)
      out.writeObject(split)
    }
  }

  override def readExternal(in: ObjectInput) {
    val stageId = in.readInt()
    val numBytes = in.readInt()
    val bytes = new Array[Byte](numBytes)
    in.readFully(bytes)
    val (rdd_, dep_) = ShuffleMapTask.deserializeInfo(stageId, bytes)
    rdd = rdd_
    dep = dep_
    partitionId = in.readInt()
    epoch = in.readLong()
    split = in.readObject().asInstanceOf[Partition]
  }

  /**
   * ShuffleMapTask的执行体
   * @param context
   * @return 记录了存放执行结果的blockManager的地址(即FileSegment的位置)MapStatus
   */
  override def runTask(context: TaskContext): MapStatus = {
    val numOutputSplits = dep.partitioner.numPartitions
    metrics = Some(context.taskMetrics)

    val blockManager = SparkEnv.get.blockManager
    val shuffleBlockManager = blockManager.shuffleBlockManager
    var shuffle: ShuffleWriterGroup = null
    var success = false

    try {
      // Obtain all the block writers for shuffle blocks.
      val ser = Serializer.getSerializer(dep.serializer)
      shuffle = shuffleBlockManager.forMapTask(dep.shuffleId, partitionId, numOutputSplits, ser)//获取所有的block writers

      // Write the map output to its associated buckets.
      //将Shuffle数据写到对应的bucket中
      for (elem <- rdd.iterator(split, context)) {//逐条写记录
        val pair = elem.asInstanceOf[Product2[Any, Any]]
        val bucketId = dep.partitioner.getPartition(pair._1)//获取bucketId
        shuffle.writers(bucketId).write(pair)//根据bucketId找到对应的bucket writer，将mapOutput的record数据写到对应的bucket中
      }

      // Commit the writes. Get the size of each bucket block (total block size).
      var totalBytes = 0L
      var totalTime = 0L
      val compressedSizes: Array[Byte] = shuffle.writers.map { writer: BlockObjectWriter =>
        writer.commit()
        writer.close()
        val size = writer.fileSegment().length
        totalBytes += size
        totalTime += writer.timeWriting()
        MapOutputTracker.compressSize(size)
      }

      // Update shuffle metrics.
      val shuffleMetrics = new ShuffleWriteMetrics
      shuffleMetrics.shuffleBytesWritten = totalBytes
      shuffleMetrics.shuffleWriteTime = totalTime
      metrics.get.shuffleWriteMetrics = Some(shuffleMetrics)

      success = true
      new MapStatus(blockManager.blockManagerId, compressedSizes)
    } catch { case e: Exception =>
      // If there is an exception from running the task, revert the partial writes
      // and throw the exception upstream to Spark.
      if (shuffle != null && shuffle.writers != null) {
        for (writer <- shuffle.writers) {
          writer.revertPartialWrites()
          writer.close()
        }
      }
      throw e
    } finally {
      // Release the writers back to the shuffle block manager.
      if (shuffle != null && shuffle.writers != null) {
        try {
          shuffle.releaseWriters(success)//善后工作
        } catch {
          case e: Exception => logError("Failed to release shuffle writers", e)
        }
      }
      // Execute the callbacks on task completion.
      context.executeOnCompleteCallbacks()
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
