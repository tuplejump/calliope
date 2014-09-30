---
layout: default
title: SnackFS - Calliope
---

# Another DFS?

SnackFS is our bite-sized, lightweight HDFS compatible FileSystem built over Cassandra. With it's unique fat driver design it requires no additional SysOps or setup on the Cassanndra Cluster. All you have to do is point to your Cassandra cluster and you are ready to go.

The goal we are working towards with the Tuplejump Platform is to simplify and speed up Big Data projects and SnackFS provides us a major step towards that.With SnackFS we introduce a new HDFS compatible DFS that works as a dropin replacement for HDFS working with Hadoop or Spark. This is particularly targetted towards Spark users who are using Cassandra in their infrastructure.

## What's the problem with HDFS?

Some major issues we had with HDFS were,

* HDFS setup is complicated without the help of specialized tools
* In HDFS the Name Node (master) introduces a single point of failure
* Having a hot standby node and zookeeper for high availability is according to us a *hack* and not a solution. It only goes on to further complicate deployment and maintenance
* We need to setup Hadoop in our Spark/Tuplejump deployments only to use HDFS

## So how does SnackFS solve these?

SnackFS is what we call a *fat client* filesystem, in the sense that all the filesystem meta information and the logic to work with the filesystem resides in the client. SnackFS doesn't have any server-side code, except Cassandra. It relies on Cassandra for data storage, replication, reliability and high availability. The advantage is,

* Cassandra is easy to deploy, bootstrap and maintain as compared to Hadoop/HDFS
* In Cassandra their is no *master*
* With Cassandra's peer-to-peer model you could connect to any of the cluster nodes to read any file
* Since the data stored is hash partitioned we can get the same data locality in SnackFS by reading only the *local blocks* on any node


## What is the thought process behind this?

It was clear, we needed something like HDFS minus its complicated deployment and master/slave architecture and it should not rely on zookeeper or any external system for high availability. Additionally, it also should be a dropin replacement for HDFS as many of our customers and prospective users would have applications built to work with HDFS.

Since we were already using Cassandra as our (un)structured data storage backend and had experimented with Datastax's Brisk and CFS in past, we decided to go with Cassandra as the file storage engine. With this decision made our task reduced to just making a HDFS compatible DFS which stored data in Cassandra.

Moreover using SnackFS in a environment where we already have Cassandra is easy and doesn't add any overhead to the IT teams.


## What do I do now?
Here are some pointers for you,

* If you are interested in reading further about the SnackFS's design and workings, [read about it here](snackfs-design.html).
* If you just want to dive in and use SnackFS, this will help you [get started](snackfs-usage.html).
* To take a look at SnackFS codebase, please signup for [Calliope early access](https://docs.google.com/forms/d/1jFTqKnp_13vTjXwy3Zex58X1JKRsFJLLWNhyZ9mQUDg/viewform). If you have already signed up, you should be able to access SnackFS [codebase here](https://github.com/tuplejump/snackfs)






