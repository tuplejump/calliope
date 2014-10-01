package com.tuplejump.calliope.sql


object CalliopeSqlSettings {
  final val enableStargateKey: String = "calliope.stargate.enable"

  final val cassandraHostKey = "spark.cassandra.connection.host"

  final val cassandraNativePortKey = "spark.cassandra.connection.native.port"

  final val cassandraRpcPortKey = "spark.cassandra.connection.rpc.port"

  final val cassandraUsernameKey = "spark.cassandra.auth.username"

  final val casssandraPasswordKey = "spark.cassandra.auth.password"

  final val loadCassandraTablesKey = "spark.cassandra.auto.load.tables"
}