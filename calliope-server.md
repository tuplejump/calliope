---
layout: default
title: Calliope Reporting Server - Calliope
---
#All I want to do is reports!

Most of the reporting tools support either JDBC/ODBC protocol. Calliope extends the Spark's HiveThirftServer2 as CalliopeServer2 to provide support to connect to and work with the data in Casssandra.

The easiest way to get started using Calliope Server is to download it [from here](http://downloads.tuplejump.com/calliope-server-assembly-1.1.0-CTP-U2.jar).

Now you can go to your Spark Installation Folder and run the following,

```sh

$ bin/spark-submit --class com.tuplejump.calliope.server.CalliopeServer2 /path/to/calliope-server-assembly-1.1.0-CTP-U2-SNAPSHOT.jar

```

Additionally, you can pass the calliope configuration using **--conf**. These settings are the same as the Calliope settings.

* __spark.cassandra.connection.host__ - Configures the initial contact point in the Cassandra cluster. Must be reachable from the driver application node. _[Default: 127.0.0.1]_
* __spark.cassandra.connection.native.port__ - The native protocol port that the contact node is listening to. _[Default: 9042]_
* __spark.cassandra.connection.rpc.port__ - The thrift protocol port that the contact node is listening to. _[Default: 9160]_
* __spark.cassandra.auth.username__ - Username for authenticating to the C\* 
* __spark.cassandra.auth.password__ - Password for authenticating to the C\* 

So to start the CalliopeServer pointing to a Cassandra Server with seed on 192.168.10.1, you will use,

```sh

$ bin/spark-submit --conf "spark.cassandra.connection.host=192.168.10.1" --class com.tuplejump.calliope.server.CalliopeServer2 /path/to/calliope-server-assembly-1.1.0-CTP-U2-SNAPSHOT.jar

```

Following this you can connect to the server using **beeline** as you would to the standard HiveThriftServer2. 

Once on the beeline console, all the Cassandra tables are already available for you. So to count the number of rows for table employees in keyspace org, you can run the following query,

```sql

SELECT COUNT(1) FROM org.employees;

```


You may  download the JDBC Driver Bundle [from here](http://downloads.tuplejump.com/calliope-jdbc-assembly-1.1.0-CTP-U2.jar). The JDBC connection settings are as follows,

* Connection URL: jdbc:hive2://localhost:10000/
* Driver Class: org.apache.hive.jdbc.HiveDriver
* Username: Your linux username on the Calliope Server system
* Password: None (Leave empty/Do not set)

You can also connect to it using **SquirrelSQL Client** and your Cassandra Tables will be listed under CASSANDRA_TABLES. Similarly you can connect to it using **Jasper**, **Birt**, etc.

For connecting to Tableau or any other tool that uses ODBC you will need to get the DataBricks ODBC driver.



