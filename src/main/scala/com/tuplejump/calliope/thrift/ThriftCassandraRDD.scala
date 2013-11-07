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

package com.tuplejump.calliope.thrift

import org.apache.hadoop.mapreduce._
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat
import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import java.text.SimpleDateFormat
import java.util.Date
import com.tuplejump.calliope.ThriftCasBuilder
import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.tuplejump.calliope.utils.CassandraPartition


class ThriftCassandraRDD[T: Manifest](sc: SparkContext,
                                                        @transient cas: ThriftCasBuilder,
                                                        unmarshaller: (ByteBuffer, Map[ByteBuffer, ByteBuffer]) => T)
  extends RDD[T](sc, Nil)
  with SparkHadoopMapReduceUtil
  with Logging {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  @transient private val conf = cas.configuration
  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))

  @transient val jobId = new JobID(System.currentTimeMillis().toString, id)

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  def compute(theSplit: Partition, context: TaskContext): Iterator[T] = new Iterator[T] {
    val conf = confBroadcast.value.value
    val format = new ColumnFamilyInputFormat
    val split = theSplit.asInstanceOf[CassandraPartition]
    //Set configuration
    val attemptId = new TaskAttemptID(jobtrackerId, id, true, split.index, 0)
    val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)


    val reader = format.createRecordReader(
      split.inputSplit.value, hadoopAttemptContext)

    reader.initialize(split.inputSplit.value, hadoopAttemptContext)
    context.addOnCompleteCallback(() => close())

    var havePair = false
    var finished = false

    override def hasNext: Boolean = {
      if (!finished && !havePair) {
        finished = !reader.nextKeyValue
        havePair = !finished
      }
      !finished
    }

    override def next: T = {
      if (!hasNext) {
        throw new java.util.NoSuchElementException("End of stream")
      }
      havePair = false
      val rowAsMap = reader.getCurrentValue.map {
        case (name, column) => column.name() -> column.value()
      }.toMap

      unmarshaller(reader.getCurrentKey, rowAsMap)
      //return (keyUnmarshaller(reader.getCurrentKey), rowUnmarshaller(rowAsMap))
    }

    private def close() {
      try {
        reader.close()
      } catch {
        case e: Exception => logWarning("Exception in RecordReader.close()", e)
      }
    }
  }

  def getPartitions: Array[Partition] = {

    val jc = newJobContext(conf, jobId)
    val inputFormat = new ColumnFamilyInputFormat()
    val rawSplits = inputFormat.getSplits(jc).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new CassandraPartition(id, i, rawSplits(i).asInstanceOf[InputSplit])
    }
    result
  }

  override protected[calliope] def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[CassandraPartition].s.getLocations
  }
}

