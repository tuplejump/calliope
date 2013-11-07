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

package com.tuplejump.calliope.utils

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.io.Writable
import org.apache.spark.{SerializableWritable, Partition}

case class CassandraPartition(rddId: Int, val idx: Int, @transient s: InputSplit) extends Partition {

  val inputSplit = new SerializableWritable(s.asInstanceOf[InputSplit with Writable])

  override def hashCode(): Int = (41 * (41 + rddId) + idx)

  override val index: Int = idx
}