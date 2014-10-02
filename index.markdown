---
layout: default
title: "Calliope, by tuplejump"
---

## Project Updates 

* Calliope Community Technology Preview (Update 2) is public, brings SparkSQL for Cassandra
* SnackFS going to GA soon. The source code is available at [snackfs-release](https://github.com/tuplejump/snackfs-release)
* Calliope [update release](getting-started.html) with minor bug fixes 
* Early Access release to [SnackFS 0.6.2](snackfs.html), our bitte sized Cassandra (1.2.x) backed DFS (Cross compiled with Scala 2.9.x and 2.10.x) is available for download.
* New [Early Access release 0.9.0-C2-EA](getting-started.html) working with Scala 2.10 and Spark 0.9.0 using **Cassandra 2.0.x** is now available.
* New [Early Access release 0.9.0-EA](getting-started.html) working with Scala 2.10 and Spark 0.9.0 is out.
* Calliope [Release 0.8.1](getting-started.html) working with Scala 2.9.3 and Spark 0.8.1 is GA

## Welcome to Calliope
Spark with Cassandra is really 'magical'. You can try it yourself and see why this makes a potent combination. You can use Spark on Cassandra without Calliope, all Calliope does is makes the magic easier!

Calliope is a library providing an interface to consume data from Cassandra to spark and store Resilient Distributed Datasets (RDD) from Spark to Cassandra.

> From Wikipedia,
>
> In Greek mythology, Calliope (/kəˈlaɪ.əpiː/ kə-ly-ə-pee; Ancient Greek: Καλλιόπη Kalliopē "beautiful-voiced") was the muse of epic poetry,daughter of Zeus and Mnemosyne, and is believed to be Homer's muse, the inspiration for the Odyssey and the Iliad.

We hope Calliope will also be the muse of your epic data poetry! 

## Why Cassandra + Spark?
Cassandra + Spark is the match made in heaven! Spark with its in memory mapreduce allows us to process data upto 10x faster than Hadoop MapReduce, opening doors to iterative map reduce, complex process chains in a plain and simple start and so much more. Spark did away with the complex setup and configuration required by Hadoop M/R in its early days. Overall, it makes big data crunching fun!

The only bottleneck now is the HDFS or worse HBASE, which are still used by many Spark developers to build the applications and providing a distributed data store to the RDD. Setting up and maintaining HDFS and maintaining the cluster, requires effort and experience. All HDFS provides is a filesystem. Anything you put there is a file and will be read line by line. This may work for unstructured data, but not so much with structured one. The problem with both these solutions is they are Hadoop! There I said it!!!

Come in Cassandra, built on Dynamo's gossip with BigTable's column oriented storage, Cassandra provides a resilient fault tolerant robust very high speed data store. Coming from the NoSQL family of databases, it provides flexible schema support, which makes it good for structured as well as unstructured data. Setting up and managing Cassandra cluster's is quick and easy.

Cassandra storage backend with Spark will open many new avenues.

## Why Calliope?
Spark supports any Hadoop Input/Output provider and we know there is a Hadoop I/O for Cassandra, so I can simply use it! 
Yes, sure you can, and for now Calliope uses the same. Unlike the Hadoop I/O API which was designed for Java and Hadoop way of doing things, Calliope provides a improved cleaner API to create and persist RDDs, without exposing you to the internals. In future we would like to move away from Hadoop I/O here and build our own fat-free alternative. But, don't worry, we won't change the API for that.
