---
layout: default
title: Getting Started - Calliope
---

# Lights, Camera, Action!

This assumes you have a working Spark and Cassandra setup. Ideally you would install your cluster such that the spark workers run on the same servers as the cassandra nodes, to ensure best data locality and reduce network traffic.

## CTP Usage

### Spark Shell

To get started using spark-shell, add [this assembly jar](http://downloads.tuplejump.com/calliope-core-assembly-1.1.0-CTP-U2.jar) to Spark shell's and executor's classpath.

### Maven dependency

```xml

<dependency>
    <groupId>com.tuplejump</groupId>
    <artifactId>calliope-core_2.10</artifactId>
    <version>1.1.0-CTP-U2</version>
</dependency>

```

### SBT Dependency

```scala

libraryDependencies += "com.tuplejump" % "calliope-core_2.10" % "1.1.0-CTP-U2"

```


