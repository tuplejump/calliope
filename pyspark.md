---
layout: default
title: PySpark - Calliope
---
#PySpark Support
**Alpha Component**

Since the launch of Calliope many people have been requesting PySpark support for it. There have been various challenges related to implementing the transformers for vanilla Calliope and exposing an API for the same in Python. But with launch of CalliopeSQL (i.e. SparkSQL support in Calliope) we have a way to reliably work with Cassandra data in PySpark without the need to write transformers.

With CalliopeSQL support for PySpark, you can work with your data in Cassandra just as you would with data in any SparkSQL supported datastore (hdfs, json files, parquet, etc.) and at the same time take advantage of the access patterns and capabilities of Cassandra.

##Download the Binaries

Download [calliope-sql-assembly](http://downloads.tuplejump.com/calliope-sql-assembly-1.1.0-CTP-U2.jar) or [calliope-hive-assembly](http://downloads.tuplejump.com/calliope-hive-assembly-1.1.0-CTP-U2.jar)

Download the [Calliope Python Egg](http://downloads.tuplejump.com/calliope-0.0.1-py2.7.egg)

##Getting Started

###PySpark Shell
Assuming you downloaded the required binaries to a folder called **calliope** in your SPARK_HOME, to start PySpark shell with *calliope-sql* support, use the following command in SPARK_HOME folder.

```sh

$ bin/pyspark --jars calliope/calliope-sql-assembly-1.1.0-CTP-U2.jar --driver-class-path calliope/calliope-sql-assembly-1.1.0-CTP-U2.jar --py-files calliope/calliope-0.0.1-py2.7.egg

```

Or to start it with calliope-hive-support use this,

```sh

$ bin/pyspark --jars calliope/calliope-hive-assembly-1.1.0-CTP-U2.jar --driver-class-path calliope/calliope-hive-assembly-1.1.0-CTP-U2.jar --py-files calliope/calliope-0.0.1-py2.7.egg

```

###Reading data from Cassandra

To read data from your Cassandra keyspace "test_ks" and table, "test_cf", first you need to create the sqlContext,

```python

from calliope import CalliopeSQLContext
sqlContext = CalliopeSQLContext(sc)

```

or to create the hive context,

```python

from calliope import CalliopeHiveContext
sqlContext = CalliopeHiveContext(sc)

```

From here on you can use it as you would use the sqlContext in PySpark with one advantage, that all your C\* keyspaces and tables are already available in the context. So to read from the table,

```python

srdd = sqlContext.sql("select * from test_ks.test_cf")

```

###Configuring CalliopeSQL

By default Calliope assumes that the driver application is running on the same node as C\* and tries to connect to 127.0.0.1 as the root node. If the driver application doesn't run on the same system as the C\* then you need to configure the location using Spark properties. Calliope also provides some additional properties that can be configured as per your requirements.

* __spark.cassandra.connection.host__ - Configures the initial contact point in the Cassandra cluster. Must be reachable from the driver application node. _[Default: 127.0.0.1]_
* __spark.cassandra.connection.native.port__ - The native protocol port that the contact node is listening to. _[Default: 9042]_
* __spark.cassandra.connection.rpc.port__ - The thrift protocol port that the contact node is listening to. _[Default: 9160]_
* __spark.cassandra.auth.username__ - Username for authenticating to the C\* 
* __spark.cassandra.auth.password__ - Password for authenticating to the C\* 

##Programatically loading Cassandra Tables

Sometimes you may need to connect to different Cassandra clusters or need access to the underlying SchemaRDD. For this purpose, Calliope's SQL Context provides the *cassandraTable* method with some alternate signatures, including one where you can provide host, port, keyspace, table, username and password.

```python

srdd = sqlContext.cassandraTable("cas.host", "cas.native.port", "keyspace", "table")

```
