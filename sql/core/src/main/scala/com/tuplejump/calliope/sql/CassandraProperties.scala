package com.tuplejump.calliope.sql

import org.apache.spark.SparkContext

case class CassandraProperties(sparkContext: SparkContext) {
  val cassandraHost: String = sparkContext.getConf.get(CalliopeSqlSettings.cassandraHostKey, "127.0.0.1")

  val cassandraNativePort: String = sparkContext.getConf.get(CalliopeSqlSettings.cassandraNativePortKey, "9042")

  val cassandraRpcPort: String = sparkContext.getConf.get(CalliopeSqlSettings.cassandraRpcPortKey, "9160")

  val loadCassandraTables = sparkContext.getConf.getBoolean(CalliopeSqlSettings.loadCassandraTablesKey, false)

  val cassandraUsername = sparkContext.getConf.getOption(CalliopeSqlSettings.cassandraUsernameKey)

  val cassandraPassword = sparkContext.getConf.getOption(CalliopeSqlSettings.casssandraPasswordKey)

  val mayUseStargate = sparkContext.getConf.getBoolean(CalliopeSqlSettings.enableStargateKey, false)
}
