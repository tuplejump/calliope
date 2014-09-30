---
layout: default
title: Streaming - Calliope
---
#Spark streaming and Calliope

Spark Streaming applications are similar to regular Spark applications. The central concept is DStreams which are a continuous sequence of RDDs.

You can persist the RDDs from DStream just as you would persist regular RDDs.

##A quick swim

1. Setup your Spark Streaming

Setup your spark streaming as you would regularly. Here is an example of setting up a socket streaming and processing it.

```scala

val ssc = new StreamingContext(args(0), "NetworkStreamToCassandra", 
        Seconds(10), System.getenv("SPARK_HOME"))

val lines = ssc.socketTextStream(args(1), args(2).toInt)
val words = lines.flatMap(_.split(" "))
val wordCounts: DStream[(String, Int)] = words.map(x => (x, 1)).reduceByKey(_ + _)

```

2. Prepare C* configuration

Prepare the C* configuration of where the processed data be saved.

```scala

val cas = CasBuilder.cql3.withColumnFamily("casdemo", "words")
        .saveWithQuery("update casdemo.words set count = ?")

```

3. Create the Marshallers

We then need marshallers to help Calliope understand how to write the RDD to the C*. This will need us to create two marshallers, one for creating a key and another for the row values.

```scala

implicit def keyMarshaller(x: (String, Int)): CQLRowKeyMap = Map("word" -> x._1)
implicit def rowMarshaller(x: (String, Int)): CQLRowValues = List(x._2)

```

4. Setup the result stream to persist

Once the processing pipeline is set, you can setup a function to persist the RDDs in DStream to C* using the new API,

```scala

wordCounts.saveToCas(cas)

```

Or using the traditional approach,

```scala

wordCounts.foreach(_.cql3SaveToCassandra(cas))

```

5. Start streaming

Start the streaming

```scala

ssc.start()

```

##SnackFS  for checkpointing

One of the things that most of the other Cassandra Spark libraries miss out is the need for a DFS for streaming checkpoint in reduceByKey and updateStateByKey operations. Calliope utilizes SnackFS for this purpose. You can set the checkpoint directory to point it to [snackfs](snackfs.html).

```scala

ssc.checkpoint("snackfs://path/to/checkpoint/dir")

```


