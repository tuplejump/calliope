#The swarm of bees!

The Spark SQL parser is limited in terms of the queries you can write, and so SparkSQL provides a Hive Context, with HiveQL parser. Calliope also supports HiveContext to execute HiveQL on Cassandra. All the functionality provided by HiveContext (including the JDBC Server) is present in Calliope's HiveContext too.

##The package

Just like Calliope SQL, you need to add calliope-hive to your project's dependencies.

##Creating the Hive Context

Like the CassandraAwareSQLContext we have a CassandraAwareHiveContext, which extends the SparkSQL HiveContext to provide Cassandra relevant functionality.

```scala

val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.hive.CassandraAwareHiveContext(sc)

import sqlContext.createSchemaRDD

```

Now you have a HiveContext that you can use same as documented in the [Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

Additionaly, this HiveContext also knows about the configured \([see sql config](start-with-sql.md)\) Cassandra cluster and is all set to run your Hive Queries against it.

This, also provides the same programming API as Calliope's SQL Context, so you can create a SchemaRDD programatically.

