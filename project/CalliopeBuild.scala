import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._

object CalliopeBuild extends Build {

  lazy val VERSION = "1.1.0-CTP-U2-SNAPSHOT"

  lazy val CAS_VERSION = "2.0.9"

  lazy val THRIFT_VERSION = "0.9.1"

  lazy val SCALA_VERSION = "2.10.4" //Updating to 2.10.4 cause of Spark/SBT issue Spark [https://issues.apache.org/jira/browse/SPARK-1923]

  lazy val DS_DRIVER_VERSION = "2.0.4"

  lazy val PARADISE_VERSION = "2.0.0"

  lazy val SPARK_VERSION = "1.1.0"

  lazy val pom = {
    <scm>
      <url>git@github.com:tuplejump/calliope.git</url>
      <connection>scm:git:git@github.com:tuplejump/calliope.git</connection>
    </scm>
      <developers>
        <developer>
          <id>milliondreams</id>
          <name>Rohit Rai</name>
          <url>https://twitter.com/milliondreams</url>
        </developer>
      </developers>
  }

  val dependencies = Seq(
    "org.apache.cassandra" % "cassandra-all" % CAS_VERSION intransitive(),
    "org.apache.cassandra" % "cassandra-thrift" % CAS_VERSION intransitive(),
    "net.jpountz.lz4" % "lz4" % "1.2.0",
    "org.apache.thrift" % "libthrift" % THRIFT_VERSION exclude("org.slf4j", "slf4j-api") exclude("javax.servlet", "servlet-api"),
    "com.datastax.cassandra" % "cassandra-driver-core" % DS_DRIVER_VERSION intransitive(),
    "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided" exclude("org.apache.hadoop", "hadoop-core"),
    "org.apache.spark" %% "spark-streaming" % SPARK_VERSION % "provided",
    "org.apache.hadoop" % "hadoop-client" % "1.0.4" % "provided",
    "com.github.nscala-time" %% "nscala-time" % "1.0.0",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test"
  )

  val commonSettings = Project.defaultSettings ++ Seq(
    organization := "com.tuplejump",
    version := VERSION,
    scalaVersion := SCALA_VERSION,
    scalacOptions := "-deprecation" :: "-unchecked" :: "-feature" :: Nil,
    parallelExecution in Test := false,
    pomExtra := pom,
    publishArtifact in Test := false,
    pomIncludeRepository := {
      _ => false
    },
    publishMavenStyle := true,
    publishTo <<= version {
      (v: String) =>
        val nexus = "https://oss.sonatype.org/"
        if (v.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    homepage := Some(url("https://tuplejump.github.io/calliope")),
    organizationName := "Tuplejump, Inc.",
    organizationHomepage := Some(url("http://www.tuplejump.com")),
    resolvers ++= Seq("Akka Repository" at "http://repo.akka.io/releases/"),
    fork in Test := true,
    test in assembly := {}
  ) ++ net.virtualvoid.sbt.graph.Plugin.graphSettings

  lazy val macros: Project = Project(
    id = "calliope-macros",
    base = file("macros"),
    settings = commonSettings ++ Seq(
      version := VERSION,
      addCompilerPlugin("org.scalamacros" % "paradise" % PARADISE_VERSION cross CrossVersion.full),
      libraryDependencies ++= Seq("org.scalamacros" %% "quasiquotes" % PARADISE_VERSION,
        "com.datastax.cassandra" % "cassandra-driver-core" % DS_DRIVER_VERSION intransitive()),
      libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
      //scalacOptions := "-Ymacro-debug-lite" :: "-deprecation" :: "-unchecked" :: "-feature" :: Nil
      scalacOptions := "-deprecation" :: "-unchecked" :: "-feature" :: Nil
    )
  )

  lazy val calliope = {
    val calliopeSettings = commonSettings ++ Seq(
      name := "calliope-core",
      libraryDependencies ++= dependencies
      //javaOptions in Test := Seq("-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"),
    )

    Project(
      id = "calliope-core",
      base = file("core"),
      settings = calliopeSettings
    ) dependsOn (macros) aggregate (macros)
  }

  lazy val calliopeSql: Project = Project(
    id = "calliope-sql",
    base = file("sql/core"),
    settings = Project.defaultSettings ++ Seq(
      version := VERSION,
      libraryDependencies ++= dependencies ++ Seq("org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided")
    )
  ) dependsOn (calliope)

  lazy val calliopeHive: Project = Project(
    id = "calliope-hive",
    base = file("sql/hive"),
    settings = Project.defaultSettings ++ Seq(
      version := VERSION,
      libraryDependencies ++= dependencies ++ Seq(
        "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided",
        "org.apache.spark" %% "spark-hive" % SPARK_VERSION % "provided"
          exclude("commons-beanutils", "commons-beanutils-core")
          exclude("commons-collections", "commons-collections")
          exclude("commons-logging", "commons-logging-api"))
    )
  ) dependsOn (calliopeSql)

  lazy val calliopeServer: Project = Project(
    id = "calliope-server",
    base = file("sql/server"),
    settings = commonSettings ++ assemblySettings ++ Seq(
      version := VERSION,
      libraryDependencies ++= dependencies ++ Seq(
        /* "org.spark-project.hive" % "hive-service" % "0.12.0" exclude("org.jboss.netty", "netty")
          exclude("commons-beanutils", "commons-beanutils-core")
          exclude("commons-collections", "commons-collections")
          exclude("commons-logging", "commons-logging-api"), */
        "org.spark-project.hive" % "hive-cli" % "0.12.0" exclude("org.jboss.netty", "netty")
          exclude("commons-beanutils", "commons-beanutils-core")
          exclude("commons-collections", "commons-collections")
          exclude("commons-logging", "commons-logging-api")
          excludeAll (ExclusionRule(organization = "org.datanucleus")),
        "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided"
          exclude("commons-beanutils", "commons-beanutils-core")
          exclude("commons-collections", "commons-collections")
          exclude("commons-logging", "commons-logging-api"),
        "org.apache.spark" %% "spark-hive" % SPARK_VERSION % "provided"
          exclude("commons-beanutils", "commons-beanutils-core")
          exclude("commons-collections", "commons-collections")
          exclude("commons-logging", "commons-logging-api")
      )
    )
  ) dependsOn (calliopeHive)

  lazy val jdbcDriver: Project = Project(
    id="calliope-jdbc",
    base=file("sql/jdbc"),
    settings = commonSettings ++ assemblySettings ++  Seq(
      version := VERSION,
      libraryDependencies ++= Seq(
        "org.spark-project.hive" % "hive-jdbc" % "0.12.0" exclude("org.jboss.netty", "netty")
          exclude("commons-beanutils", "commons-beanutils-core")
          exclude("commons-collections", "commons-collections")
          exclude("commons-logging", "commons-logging-api")
          excludeAll (ExclusionRule(organization = "org.datanucleus"))
      )
    )
  )

  val root = Project("calliope-root", base = file(".")).aggregate(macros, calliope, calliopeSql, calliopeHive, calliopeServer, jdbcDriver)
}
