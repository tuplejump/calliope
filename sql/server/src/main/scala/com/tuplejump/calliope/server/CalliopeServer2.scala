package com.tuplejump.calliope.server

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import ReflectionUtils._


import scala.collection.JavaConversions._

/**
 * The main entry point for the Spark SQL port of HiveServer2.  Starts up a `SparkSQLContext` and a
 * `HiveThriftServer2` thrift server.
 */
object CalliopeServer2 extends Logging {
  var LOG = LogFactory.getLog(classOf[CalliopeServer2])

  def main(args: Array[String]) {
    val optionsProcessor = new ServerOptionsProcessor("HiveThriftServer2")

    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }

    val ss = new SessionState(new HiveConf(classOf[SessionState]))

    // Set all properties specified via command line.
    val hiveConf: HiveConf = ss.getConf
    hiveConf.getAllProperties.toSeq.sortBy(_._1).foreach { case (k, v) =>
      logDebug(s"HiveConf var: $k=$v")
    }

    SessionState.start(ss)

    logInfo("Starting SparkContext")
    CalliopeSparkSQLEnv.init()
    SessionState.start(ss)

    Runtime.getRuntime.addShutdownHook(
      new Thread() {
        override def run() {
          CalliopeSparkSQLEnv.stop()
        }
      }
    )

    try {
      val server = new CalliopeServer2(CalliopeSparkSQLEnv.hiveContext)
      server.init(hiveConf)
      server.start()
      logInfo("HiveThriftServer2 started")
    } catch {
      case e: Exception =>
        logError("Error starting HiveThriftServer2", e)
        System.exit(-1)
    }
  }
}

class CalliopeServer2(hiveContext: HiveContext)
  extends HiveServer2
  with ReflectedCompositeService {

  override def init(hiveConf: HiveConf) {
    val sparkSqlCliService = new CalliopeCLIService(hiveContext)
    setSuperField(this, "cliService", sparkSqlCliService)
    addService(sparkSqlCliService)

    val thriftCliService = new ThriftBinaryCLIService(sparkSqlCliService)
    setSuperField(this, "thriftCLIService", thriftCliService)
    addService(thriftCliService)

    initCompositeService(hiveConf)
  }
}
