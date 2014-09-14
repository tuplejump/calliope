import sbt.Keys._
import sbt._

object
CalliopeBuild extends Build {

  lazy val VERSION = "0.9.5-EA-SNAPSHOT"

  lazy val CAS_VERSION = "2.0.9"

  lazy val THRIFT_VERSION = "0.9.1"

  lazy val SCALA_VERSION = "2.10.4" //Updating to 2.10.4 cause of Spark/SBT issue Spark [https://issues.apache.org/jira/browse/SPARK-1923]

  lazy val DS_DRIVER_VERSION = "2.0.4"

  lazy val PARADISE_VERSION = "2.0.0"

  lazy val SPARK_VERSION = "1.0.2"

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

  lazy val calliope = {
    val dependencies = Seq(
      "org.apache.cassandra" % "cassandra-all" % CAS_VERSION % "provided" intransitive(),
      "org.apache.cassandra" % "cassandra-thrift" % CAS_VERSION % "provided" intransitive(),
      "net.jpountz.lz4" % "lz4" % "1.2.0",
      "org.apache.thrift" % "libthrift" % THRIFT_VERSION exclude("org.slf4j", "slf4j-api") exclude("javax.servlet", "servlet-api"),
      "com.datastax.cassandra" % "cassandra-driver-core" % DS_DRIVER_VERSION intransitive(),
      "org.apache.spark" %% "spark-core" % SPARK_VERSION % "provided" exclude("org.apache.hadoop", "hadoop-core"),
      "org.apache.spark" %% "spark-streaming" % SPARK_VERSION % "provided",
      "org.apache.spark" %% "spark-sql" % SPARK_VERSION % "provided",
      "org.apache.hadoop" % "hadoop-client" % "1.0.4" % "provided",
      "com.github.nscala-time" %% "nscala-time" % "1.0.0",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test"
    )

    val calliopeSettings = Seq(
      name := "calliope",

      organization := "com.tuplejump",

      version := VERSION,

      scalaVersion := SCALA_VERSION,

      crossScalaVersions := Seq("2.10.4"),

      scalacOptions := "-deprecation" :: "-unchecked" :: "-feature" :: Nil,

      libraryDependencies ++= dependencies,

      parallelExecution in Test := false,

      pomExtra := pom,

      publishArtifact in Test := false,

      pomIncludeRepository := {
        _ => false
      },

      publishMavenStyle := true,

      retrieveManaged := true,

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

      //javaOptions in Test := Seq("-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"),

      fork in Test := true
    )

    Project(
      id = "calliope",
      base = file("."),
      settings = Project.defaultSettings ++ calliopeSettings ++ net.virtualvoid.sbt.graph.Plugin.graphSettings
    ) dependsOn (macros) aggregate (macros)
  }

  lazy val macros = Project(
    id = "calliope-macros",

    base = file("macros"),

    settings = Project.defaultSettings ++ Seq(
      version := VERSION,

      addCompilerPlugin("org.scalamacros" % "paradise" % PARADISE_VERSION cross CrossVersion.full),

      libraryDependencies ++= Seq("org.scalamacros" %% "quasiquotes" % PARADISE_VERSION,
        "com.datastax.cassandra" % "cassandra-driver-core" % DS_DRIVER_VERSION intransitive()),

      libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),

      scalacOptions := "-Ymacro-debug-lite" :: "-deprecation" :: "-unchecked" :: "-feature" :: Nil,

      organization := "com.tuplejump",

      scalacOptions := "-deprecation" :: "-unchecked" :: "-feature" :: Nil,

      parallelExecution in Test := false,

      pomExtra := pom,

      publishArtifact in Test := false,

      pomIncludeRepository := {
        _ => false
      },

      publishMavenStyle := true,

      retrieveManaged := true,

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

      organizationHomepage := Some(url("http://www.tuplejump.com"))
    )
  )
}
