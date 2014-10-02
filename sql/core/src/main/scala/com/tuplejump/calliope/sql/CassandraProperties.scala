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

package com.tuplejump.calliope.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class CassandraProperties(sparkContext: SparkContext) {
  val conf: SparkConf = sparkContext.getConf

  val cassandraHost: String = conf.get(CalliopeSqlSettings.cassandraHostKey, "127.0.0.1")

  val cassandraNativePort: String = conf.get(CalliopeSqlSettings.cassandraNativePortKey, "9042")

  val cassandraRpcPort: String = conf.get(CalliopeSqlSettings.cassandraRpcPortKey, "9160")

  val loadCassandraTables = conf.getBoolean(CalliopeSqlSettings.loadCassandraTablesKey, false)

  val cassandraUsername = conf.getOption(CalliopeSqlSettings.cassandraUsernameKey)

  val cassandraPassword = conf.getOption(CalliopeSqlSettings.casssandraPasswordKey)

  val mayUseStargate = conf.getBoolean(CalliopeSqlSettings.enableStargateKey, false)
}
