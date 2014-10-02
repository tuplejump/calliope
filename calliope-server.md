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

Following this you can connect to the server using **beeline** as you would to the standard HiveThriftServer2. 

You may  download the JDBC Driver Bundle [from here](http://downloads.tuplejump.com/calliope-jdbc-assembly-1.1.0-CTP-U2.jar). The JDBC connection settings are as follows,

* Connection URL: jdbc:hive2://localhost:10000/
* Driver Class: org.apache.hive.jdbc.HiveDriver
* Username: Your linux username on the Calliope Server system
* Password: None (Leave empty/Do not set)

You can also connect to it using **SquirrelSQL Client** and your Cassandra Tables will be listed under CASSANDRA_TABLES. Similarly you can connect to it using **Jasper**, **Birt**, etc.

For connecting to Tableau or any other tool that uses ODBC you will need to get the DataBricks ODBC driver.



