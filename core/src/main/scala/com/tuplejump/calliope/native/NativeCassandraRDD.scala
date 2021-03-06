/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.tuplejump.calliope.native

import java.text.SimpleDateFormat
import java.util.Date

import com.datastax.driver.core.Row
import com.tuplejump.calliope.hadoop.cql3.{CqlConfigHelper, CqlInputFormat}
import com.tuplejump.calliope.utils.{CassandraPartition, SparkHadoopMapReduceUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputSplit, JobID, TaskAttemptID}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

private[calliope] class NativeCassandraRDD[T: ClassTag](sc: SparkContext,
                                                        confBroadcast: Broadcast[SerializableWritable[Configuration]],
                                                        unmarshaller: Row => T)
  extends RDD[T](sc, Nil)
  with SparkHadoopMapReduceUtil
  with Logging {
  @transient val jobId = new JobID(System.currentTimeMillis().toString, id)

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  def compute(theSplit: Partition, context: TaskContext): Iterator[T] = new Iterator[T] {
    val conf = confBroadcast.value.value
    val format = new CqlInputFormat
    val split = theSplit.asInstanceOf[CassandraPartition]
    logInfo("Input split: " + split.inputSplit)

    logInfo("Using MultiRangeSplit: " + CqlConfigHelper.getMultiRangeInputSplit(conf))

    //Set configuration
    val attemptId = new TaskAttemptID(jobtrackerId, id, true, split.index, 0)
    val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)

    logInfo(s"Will create record reader for ${format}")
    val reader = format.createRecordReader(split.inputSplit.value, hadoopAttemptContext)

    reader.initialize(split.inputSplit.value, hadoopAttemptContext)
    //context.addOnCompleteCallback(() => close())
    context.addTaskCompletionListener(f => close())

    var havePair = false
    var finished = false

    var timeToReadRows = 0L
    var timeToUnmarshall = 0L
    var rowsRead = 0L

    override def hasNext: Boolean = {
      if (!finished && !havePair) {
        finished = !reader.nextKeyValue
        havePair = !finished
      }
      !finished
    }

    override def next: T = {
      val rowReadStartTime =  System.nanoTime()
      if (!hasNext) {
        throw new java.util.NoSuchElementException("End of stream")
      }
      havePair = false

      val row = reader.getCurrentValue

      val rowReadEndTime =  System.nanoTime()

      val obj = unmarshaller(reader.getCurrentValue)

      val rowUnmarshellTime = System.nanoTime()
      timeToReadRows += (rowReadEndTime - rowReadStartTime)

      timeToUnmarshall += (rowUnmarshellTime - rowReadEndTime)

      rowsRead += 1

      obj
    }

    private def close() {
      logInfo(s"READ: $rowsRead | TIME TO READ: ${timeToReadRows/1000} micros | TIME TO UNMARSHALL: ${timeToUnmarshall/1000} micros")
      try {
        reader.close()
      } catch {
        case e: Exception => logWarning("Exception in RecordReader.close()", e)
      }
    }
  }

  def getPartitions: Array[Partition] = {

    logInfo("Building partitions")
    val jc = newJobContext(confBroadcast.value.value, jobId)
    val inputFormat = new CqlInputFormat
    val rawSplits = inputFormat.getSplits(jc).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new CassandraPartition(id, i, rawSplits(i).asInstanceOf[InputSplit])
    }
    logInfo(s"Got ${result.length} partitions ")
    result
  }

  override protected[calliope] def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[CassandraPartition].s.getLocations
  }
}
