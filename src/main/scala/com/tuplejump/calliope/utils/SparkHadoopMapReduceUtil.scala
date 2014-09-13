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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{JobContext, JobID, TaskAttemptContext, TaskAttemptID}

trait SparkHadoopMapReduceUtil {
  def newJobContext(conf: Configuration, jobId: JobID): JobContext = {
    val klass = firstAvailableClass(
      "org.apache.hadoop.mapreduce.task.JobContextImpl", // hadoop2, hadoop2-yarn
      "org.apache.hadoop.mapreduce.JobContext") // hadoop1
    val ctor = klass.getDeclaredConstructor(classOf[Configuration], classOf[JobID])
    ctor.newInstance(conf, jobId).asInstanceOf[JobContext]
  }

  def newTaskAttemptContext(conf: Configuration, attemptId: TaskAttemptID): TaskAttemptContext = {
    val klass = firstAvailableClass(
      "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl", // hadoop2, hadoop2-yarn
      "org.apache.hadoop.mapreduce.TaskAttemptContext") // hadoop1
    val ctor = klass.getDeclaredConstructor(classOf[Configuration], classOf[TaskAttemptID])
    ctor.newInstance(conf, attemptId).asInstanceOf[TaskAttemptContext]
  }

  private def firstAvailableClass(first: String, second: String): Class[_] = {
    try {
      Class.forName(first)
    } catch {
      case e: ClassNotFoundException =>
        Class.forName(second)
    }
  }

}
