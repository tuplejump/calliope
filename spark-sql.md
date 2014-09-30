#SQL is my Language of choice to work with data!

**Community Technology Preview**

SparkSQL is the new cool kid on the block. Beating all performance benchmarks and providing a new level easy and power to work with big data through SQL and LINQ. With the latest version, Calliope brings SparkSQL query optimization and execution for Cassandra. 

In future we see CalliopeSQL becoming the default and defacto API for consuming data from Cassandra to Spark.

*Note*: SparkSQL APIs to integrate third party datastores are currently not defined/finalized and Calliope relies on internal/developer APIs and some hacks to get this working. Over time, we will work with Databricks and as SparkSQL and Calliope mature we will be refactoring the internals to use standard APIs and optimize the query planning further. But the good part is that your programs written using this API will remain unchanged and automatically benefit from the improvements.
