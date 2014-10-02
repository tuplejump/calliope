#Get, Set, Go!

If you are just experimenting using the Spark Shell, add [this assembly](http://downloads.tuplejump.com/calliope-sql-assembly-1.1.0-CTP-U2.jar) jar to the shell and executor jars.

```sh

$ bin/spark-shell --jars calliope-sql-assembly-1.1.0-CTP-U2.jar

```

To use CalliopeSQL in your project you need to add the calliope-sql package to the project dependencies. 

Maven: 
```xml

<dependency>
    <groupId>com.tuplejump</groupId>
    <artifactId>calliope-sql_2.10</artifactId>
    <version>1.1.0-CTP-U2</version>
</dependency>

```

SBT:
```scala

libraryDependencies += "com.tuplejump" %% "calliope-sql" % "1.1.0-CTP-U2"

```


##Create the Calliope SQL Context

Currently, due certain limitations in SparkSQL implementation we cannot provide the Cassandra support directly on SQLContext, so we provide a custom SQLContext (CassandraAwareSQLContext) extending SparkSQL's SQLContext. Creating a CassandraAwareSQLContext is, no surprise, same as creating a SQLContext.

```scala

val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.CassandraAwareSQLContext(sc)

import sqlContext.createSchemaRDD

```

Now you have a SQLContext that you can use same as documented in the [Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

##Querying Cassandra

With Calliope's SQLContext you can directly access Cassandra tables in your SQL queries. 

```scala
//Assuming you have a table called 'employees' in a Cassandra keyspace 'org'
sqlContext.sql("SELECT * FROM org.employees")

```

And you can run any SQL query on this that Spark SQL can handle. The good part is as Calliope understands what queries C\* can and cannot handle it will rwrite the queries aand combine SparkSQL computation along to give the best shot at getting fast results. It will autmatically handle the utilization of Clustering Keys and Indexes in the queries, thus reducing the amount of data needed to be transferred from C\* to Spark.

##Configuring CalliopeSQL

By default Calliope assumes that the driver application is running on the same node as C\* and tries to connect to 127.0.0.1 as the root node. If the driver application doesn't run on the same system as the C\* then you need to configure the location using Spark properties. Calliope also provides some additional properties that can be configured as per your requirements.

* __spark.cassandra.connection.host__ - Configures the initial contact point in the Cassandra cluster. Must be reachable from the driver application node. _[Default: 127.0.0.1]_
* __spark.cassandra.connection.native.port__ - The native protocol port that the contact node is listening to. _[Default: 9042]_
* __spark.cassandra.connection.rpc.port__ - The thrift protocol port that the contact node is listening to. _[Default: 9160]_
* __spark.cassandra.auth.username__ - Username for authenticating to the C\* 
* __spark.cassandra.auth.password__ - Password for authenticating to the C\* 

##Programatically loading Cassandra Tables

Sometimes you may need to connect to different Casssandra clusters or need access tto the underlying SchemaRDD. For this purpose, Calliope's SQL Context provides the cassandraTable method with some alternate signatures, including one where you can provide host, port, keyspace, table, username and password.

```scala

sqlContext.cassandraTable("cas.host", "cas.native.port", "keyspace", "table", "username", "password")

```

