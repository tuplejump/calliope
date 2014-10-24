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


object CalliopeSqlSettings {
  final val enableStargateKey: String = "spark.calliope.stargate.enable"

  final val cassandraHostKey = "spark.cassandra.connection.host"

  final val cassandraNativePortKey = "spark.cassandra.connection.native.port"

  final val cassandraRpcPortKey = "spark.cassandra.connection.rpc.port"

  final val cassandraUsernameKey = "spark.cassandra.auth.username"

  final val casssandraPasswordKey = "spark.cassandra.auth.password"

  final val loadCassandraTablesKey = "spark.cassandra.auto.load.tables"

  final val rangesPerSplit = "spark.calliope.ranges.per.split"
}