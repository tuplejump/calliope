---
layout: default
title: Community Technology Preview - Calliope
---
#A peek into the future!

We started the Calliope project over a year back, when no straight forward solution was available for working with Cassandra Data in Spark. Over the last year, we have worked on stabilizing the core and have seen it being adopted and deployed at many organizations. We laid the foundation of and inspired the path of every other Spark-Cassandra connector out there, and we see it as success of the Open Source model that there is now a thriving community around the Spark on C\* community.

In the same spirit, which led us to create and release Calliope back then, we take a step forward, with Calliope Community Technology Preview release. This release brings a series of builds to give an early peek into what is to come in Tuplejump's Big Data Platform and will help the early adopters and technology leaders to utilize the latest and greatest in Calliope and will also be the beacon for all the other connectors on the path into the future.

##1.1.0 Community Technology Preview (Update 2)

This is the first external release in the CTP series and the theme of this release is SparkSQL integration. With this build we are making available the first SparkSQL implementation working with Cassandra. This includes a fully functional SparkSQL mod that enables us to run unmodified SparkSQL and HiveQL on data in Cassandra. The implementation, closely mimics the native Parquet integration released in Spark 1.1.0. 

[The core](start-with-sql.html) of this feature is a query rewriter, that can work with SparkSQL plan to run appropriate query against Cassandra (within the limitations of CQL) and process the rest in SparkSQL, thus utilizing the C\* indexes (or Stargate indexes) still providing the unrestrited querying capabilities of Spark/Hive SQL.

[The HiveQL](start-with-hive.html) integration is designed on the lines of new and experimental Parquet HiveQL integration that builds Spark SchemaRDD using SparkSQL plans, relations and table scan mechanism instead of redirecting through Hive like we do in [CASH](http://github.com/tuplejump/cash).

The [Calliope Reporting Server](calliope-server.html) provides a packaged HiveThriftServer2 compatible JDBC/ODBC server built on SparkSQL HiveThriftServer2 that allows you to connect and query the data in Cassandra (and Hive) from SQL Clients and BI and Report builder tools that support JDBC/ODBC.

