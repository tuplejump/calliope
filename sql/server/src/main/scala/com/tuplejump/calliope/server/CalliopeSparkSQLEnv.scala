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
