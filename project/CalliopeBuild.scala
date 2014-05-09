import sbt._
import sbt.Keys._
import scala.xml.NodeSeq

object CalliopeBuild extends Build {

  lazy val USE_CASV2 = System.getenv("USE_CASV2") != null && System.getenv("USE_CASV2").equalsIgnoreCase("true")

  lazy val VERSION = "0.9.0-U1-" + (if (USE_CASV2) "C2-EA" else "EA")

  lazy val CAS_VERSION = if (USE_CASV2) "2.0.7" else "1.2.16"

  lazy val THRIFT_VERSION = if (USE_CASV2) "0.9.1" else "0.7.0"

  lazy val SCALA_VERSION = "2.10.3"

  lazy val DS_DRIVER_VERSION = "2.0.1"

  def sparkDependency(scalaVer: String) =
    scalaVer match {
      case "2.10.3" =>
        Seq("org.apache.spark" %% "spark-core" % "0.9.0-incubating",
          "org.apache.spark" %% "spark-streaming" % "0.9.0-incubating" % "provided")

      case x =>
        Seq("org.apache.spark" %% "spark-core" % "0.9.0-incubating",
          "org.apache.spark" %% "spark-streaming" % "0.9.0-incubating" % "provided")
    }

  lazy val calliope = {
    val dependencies = Seq(
      "org.apache.cassandra" % "cassandra-all" % CAS_VERSION intransitive(),
      "org.apache.cassandra" % "cassandra-thrift" % CAS_VERSION intransitive(),
      "org.apache.thrift" % "libthrift" % THRIFT_VERSION exclude("org.slf4j", "slf4j-api") exclude("javax.servlet", "servlet-api"),
      "com.datastax.cassandra" % "cassandra-driver-core" % DS_DRIVER_VERSION,
      "org.slf4j" % "slf4j-jdk14" % "1.7.5",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test"
    )


    val pom = {
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

    val calliopeSettings = Seq(
      name := "calliope",

      organization := "com.tuplejump",

      version := VERSION,

      scalaVersion := SCALA_VERSION,

      crossScalaVersions := Seq("2.10.3"),

      scalacOptions <<= scalaVersion map {
        v: String =>
          val default = "-deprecation" :: "-unchecked" :: Nil
          if (v.startsWith("2.9.")) default else default :+ "-feature"
      },

      libraryDependencies ++= dependencies,

      libraryDependencies <++= (scalaVersion)(sparkDependency),

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

      resolvers ++= Seq("Akka Repository" at "http://repo.akka.io/releases/")
    )

    Project(
      id = "calliope",
      base = file("."),
      settings = Project.defaultSettings ++ calliopeSettings ++ net.virtualvoid.sbt.graph.Plugin.graphSettings
    )
  }
}
