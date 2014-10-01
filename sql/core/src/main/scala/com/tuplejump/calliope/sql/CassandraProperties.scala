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
