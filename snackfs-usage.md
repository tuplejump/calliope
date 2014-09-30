---
layout: default
title: SnackFS Usage - Calliope
---

# I can't wait to get my hands dirty! How do I start using SnackFS?

All you need to start using SnackFS is a C\* cluster (or just one instance) and SnackFS distribution.

## How do I install SnackFS?

You can download the binary distribution of SnackFS for your version of Scala,

* [Scala 2.9.x and Cassandra 1.2.x](http://bit.ly/1kZGAEL)
* [Scala 2.10.x and Cassandra 1.2.x](http://bit.ly/1f6AvRH)
* [Scala 2.9.x and Cassandra 2.0.x](http://bit.ly/1nbJiZ2)
* [Scala 2.10.x and Cassandra 2.0.x](http://bit.ly/1f6BjGj)

and extract it on a node running C\*. That's all there is! For your convenience you may set the SNACKFS_HOME environment variable to point to the extracted directory. Checkout the [Commands](#snfscmd) below to get started. Read up on [Configuration](#snfsconfig) if you need to change some settings like connecting to a remote C\* cluster.

## How do I add dependencies to my SBT/Maven Project?

* To add SnackFS to your SBT project use,

For SBT
```scala
"com.tuplejump" %% "snackfs" % "[SNACKFS_VERSION]"
```

* To add SnackFS to your Maven project use this snippet with the appropriate Scala and SnackFS version

```xml
<dependency>
  <groupId>com.tuplejump</groupId>
  <artifactId>snackfs_[SCALA_VERSION]</artifactId>
  <version>[SNACKFS_VERSION]</version>
</dependency>
```

Where SnackFS version is **0.6.2-EA** for use with *Cassandra 1.2.x*
And **0.6.2-C2-EA** for use with *Cassandra 2.0.x*

The Scala version is **2.9.3** for *Scala 2.9.x* and **2.10** for use with *Scala 2.10.x*


## How do I upload a file to SnackFS?

You can use either the -put command or the -copyFromLocal command to move
a local file or directory into SnackFS. For example, if you plan to move the
local directory /var/tuplejump/app/logs into the distributed file system
TargetDir directory,
using the put command:

```
snackfs -put /var/tuplejump/app/logs snackfs:///TargetDir/
```

Using the copyFromLocal command:

```
snackfs -copyFromLocal /var/tuplejump/app/logs snackfs:///TargetDir/
```

## How do I download a file?
You can use the -get command or the -copyToLocal command to copy a file from
SnackFS to the local system. For example, if you plan to copy /app/logs into
the local /var/tuplejump/app/TargetDir/ directory,
using the get command:

```
snackfs -get snackfs:///app/logs /var/tuplejump/app/TargetDir/
```

using copyToLocal command:

```
snackfs -copyToLocal snackfs:///app/logs /var/tuplejump/app/TargetDir/
```

## How do I list files in a directory?
You can use the -ls command to list the contents of a directory. For example,
if you want to view the list for /app directory,

```
snackfs -ls snackfs:///app
```

And to view a list of contents recursively, you can use the -lsr command.

##<a name="snfscmd"></a> What commands can I use with SnackFS?
The following Hadoop Shell commands are supported in SnackFS. These are inspired by the Unix Filesystem commands and behave similarly.

1. ls, lsr
2. du, dus, stat
3. cp
4. mv
5. mkdir
6. rm, rmr
7. cat
8. tail
9. put
10. copyFromLocal
11. getmerge
12. get
13. copyToLocal
14. touchz
15. test
16. count
17. text
18. help

The help command can be used to see the usage of all or individual commands. For example,

```
snackfs -help
```

will display help for all the supported commands, and to see help for mkdir,

```
snackfs -help mkdir
```

##<a name="snfsconfig"></a> How do I configure SnackFS?

SnackFS is configured using the core-site.xml present in the **conf** directorry in your SNACKFS_HOME. The format of this file is same as Hadoop's cconfiguration XML files, containing properties that have a name and value.

## How do I use a remote C\* cluster with SnackFS?

Edit **conf/core-site.xml** and change the **snackfs.cassandra.host** to point to a node in your remote C\* cluster.

## What are the configuration properties in ```conf/core-site.xml```, what do they mean?

* **snackfs.cassandra.host (default 127.0.0.1)**
The IP Address of the C\* node to be used for communication.

* **snackfs.cassandra.port (default 9160)**
The port on which the C\* Thrift server is listening on the node.

* **snackfs.keyspace (default snackfs)**
The name of the C\* keyspace to use for the storing the SnackFS metadata and Data blocks. This will be created is it doesn't already exist.

* **snackfs.replicationFactor (default 3)**
The replication factor for the data in SnackFS. This is used to set the replication factor for the SnackFS Keyspace in C\*. Changing it after creation of the keyspace won't have any affect.

* **snackfs.replicationStrategy (default org.apache.cassandra.locator.SimpleStrategy)**
The replication strategy for the data in SnackFS. This is used to set the replication strategy for the SnackFS Keyspace in C\*. Changing it after creation of the keyspace won't have any affect.

* **snackfs.consistencyLevel.write (default QUORUM)**
The consistency level to be used while writing the data to SnackFS blocks in C\*.

* **snackfs.consistencyLevel.read (default QUORUM)**
The consistency level to be used while reading the data from SnackFS blocks in C\*.

* **snackfs.subblock.size (default 8 MB [8 * 1024 * 1024])**
The maximum amount of data to be stored in one **snackfs subblock** i.e. one C\* cell in **sblock table**.  Checkout the [SnackFS Design](snackfs-design.html) for details.

* **snackfs.block.size (default 128 MB [128 * 1024 * 1024])**
The maximum amount of data to be stored in one **snackfs block** i.e. one C\* row in **sblock table**.  Checkout the [SnackFS Design](snackfs-design.html) for details.

