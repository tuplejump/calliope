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

package com.tuplejump.calliope.server

import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.sql.hive.{CassandraAwareHiveContext, HiveContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 *
 * This class replicates the logic from Spark's SparlSQLEnv. It is just used so that
 * we can provide CassandraAwareHiveContext for CalliopeThriftServer2.
 *
 */

/** A singleton object for the master program. The slaves should not access this. */
object CalliopeSparkSQLEnv extends Logging {
  logDebug("Initializing SparkSQLEnv")

  var hiveContext: HiveContext = _
  var sparkContext: SparkContext = _

  def init() {
    if (hiveContext == null) {
      sparkContext = new SparkContext(new SparkConf()
        .setAppName(s"SparkSQL::${java.net.InetAddress.getLocalHost.getHostName}"))

      sparkContext.addSparkListener(new StatsReportListener())

      hiveContext = new CassandraAwareHiveContext(sparkContext) {
        @transient override lazy val sessionState = SessionState.get()
        @transient override lazy val hiveconf = sessionState.getConf
      }
    }
  }

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop() {
    logDebug("Shutting down Spark SQL Environment")
    // Stop the SparkContext
    if (CalliopeSparkSQLEnv.sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
      hiveContext = null
    }
  }
}
