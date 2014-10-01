Calliope
========
Calliope is a library providing an interface to consume data from Cassandra to spark and store RDDs from Spark to Cassandra.

In Greek mythology, Calliope (/kəˈlaɪ.əpiː/ kə-ly-ə-pee; Ancient Greek: Καλλιόπη Kalliopē "beautiful-voiced") was the muse of epic poetry,daughter of Zeus and Mnemosyne, and is believed to be Homer's muse, the inspiration for the Odyssey and the Iliad.

We hope Calliope will also be the muse for your epic data poetry!

For more information visit - http://tuplejump.github.com/calliope

## SNAPSHOT BUILD FOR SPARK 1.0.0

This only works with Cassandra 2.0 and you will need to enable Sonatype Snapshot repository 

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

In SBT you can do the same with these two lines,

```scala

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "com.tuplejump" %% "calliope" % "0.9.4-EA-SNAPSHOT"

```


## Download Binary

You can download the library from for Spark v0.8.1 [here](http://bit.ly/1mUWF39) and for Spark v0.9.0 [here](http://bit.ly/1c8CdHq).

Now we also have support support for Cassandra 2.0.x and you can download the binary [here](http://bit.ly/1g9SXtx)

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


**Note on Scala version**
Every version of Calliope is built with the same version as the corresponding release of Spark, i.e. Calliope 0.8.1 is built against Scala 2.9.3 and Calliope 0.9.0 against Scala 2.10.3.

