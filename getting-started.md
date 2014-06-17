---
layout: default
title: Getting Started - Calliope
---

# Lights, Camera, Action!

This assumes you have a working Spark and Cassandra setup. Ideally you would install your cluster such that the spark workers run on the same servers as the cassandra nodes, to ensure best data locality and reduce network traffic.

## Using it with spark-shell

Download the released jar, for  and add it to your Spark shell classpath and the workers using sc.addJar, or if you build Spark from trunk or using a version newer than Spark 0.7.2 you can use the ADD_JARS environment variable to do this.

## Releases

### Cassandra 1.2.x

You can download Calliope from for Spark v0.8.1 [here](http://bit.ly/1mUWF39) and for Spark v0.9.0 [here](http://bit.ly/1c8CdHq).

You will also have to add [cassandra-all](http://repo1.maven.org/maven2/org/apache/cassandra/cassandra-all/1.2.12/cassandra-all-1.2.12.jar), [cassandra-thrift](http://repo1.maven.org/maven2/org/apache/cassandra/cassandra-thrift/1.2.12/cassandra-thrift-1.2.12.jar), [libthrift](http://repo1.maven.org/maven2/org/apache/thrift/libthrift/0.7.0/libthrift-0.7.0.jar) to the classpath. Or you could just add your cassandra/lib to classpath.

### Cassandra 2.0.x

Currently this is built only in with Spark v0.9.x. You can use the release [0.9.0-C2-EA](http://bit.ly/1g9SXtx).

If you are using Spark v0.8.x, let us know and we will release a build against it.

To use this you will have to add [cassandra-all 2.0](http://central.maven.org/maven2/org/apache/cassandra/cassandra-all/2.0.5/cassandra-all-2.0.5.jar), [cassandra-thrift 2.0](http://central.maven.org/maven2/org/apache/cassandra/cassandra-thrift/2.0.4/) and [libthrift 0.9.1](http://central.maven.org/maven2/org/apache/thrift/libthrift/0.9.1/libthrift-0.9.1.jar).


## Using it in your project

Add Calliope as dependency to your project build file.

### Snapshot Build for Spark 1.0.0 and Cassandra >= 2.0.7

*Note:* Though all the functionality in past releases works, the new ffunctionality introduced in this release is still in development and not frozen.

#### Maven Project

This only works with Cassandra 2.0 and you will need to enable Sonatype Snapshot repository. 

```xml
 <repositories>
   <repository>
     <id>snapshots-repo</id>
     <url>https://oss.sonatype.org/content/repositories/snapshots</url>
     <releases><enabled>false</enabled></releases>
     <snapshots><enabled>true</enabled></snapshots>
   </repository>
 </repositories>

```

and add dependency to Calliope release 0.9.4-EA-SNAPSHOT,

```xml

  <dependency>
    <groupId>com.tuplejump</groupId>
    <artifactId>calliope_2.10</artifactId>
    <version>0.9.4-EA-SNAPSHOT</version>
  </dependency>

```

#### SBT Project

In SBT you can do the same with these two lines,

```scala

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "com.tuplejump" %% "calliope" % "0.9.4-EA-SNAPSHOT"

```


### Add to Maven

#### With Cassandra 1.2.x

Working with Spark 0.8.1 and Scala 2.9.x,

```xml
<dependency>
  <groupId>com.tuplejump</groupId>
  <artifactId>calliope_2.9.3</artifactId>
  <version>0.8.1-U1</version>
</dependency>
```


Working with Spark 0.9.0 and Scala 2.10.x,

```xml
<dependency>
  <groupId>com.tuplejump</groupId>
  <artifactId>calliope_2.10</artifactId>
  <version>0.9.0-U1-EA</version>
</dependency>
```

#### With Cassandra 2.0.x

Working with Spark 0.9.0 and Scala 2.10.x you can use the snip below. Notice the **C2** in the version number.

```xml
<dependency>
  <groupId>com.tuplejump</groupId>
  <artifactId>calliope_2.10</artifactId>
  <version>0.9.0-U1-C2-EA</version>
</dependency>
```


### Add to SBT

#### With Cassandra 1.2.x

Working with Spark 0.8.1 and Scala 2.9.x,

```scala
libraryDependencies += "com.tuplejump" %% "calliope" % "0.8.1-U1"
```


Working with Spark 0.9.0 and Scala 2.10.x,

```scala
libraryDependencies += "com.tuplejump" %% "calliope" % "0.9.0-U1-EA"
```

#### With Cassandra 2.0.x

Working with Spark 0.9.0 and Scala 2.10.x you can use the snip below. Notice the **C2** in the version number.

```scala
libraryDependencies += "com.tuplejump" %% "calliope" % "0.9.0-U1-C2-EA"
```


## Imports

Then you should import Implicits._, RichByteBuffer._ and CasBuilder in you shell or the Scala file where you want to use Calliope.

```scala
import com.tuplejump.calliope.Implicits._
import com.tuplejump.calliope.utils.RichByteBuffer._
import com.tuplejump.calliope.CasBuilder
```
