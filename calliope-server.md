#All I want to do is reports!

Most of the reporting tools support either JDBC/ODBC protocol. Calliope extends the Spark's HiveThirftServer2 as CalliopeServer2 to provide support to connect to and work with the data in Casssandra.

The easiest way to get started using Calliope Server is to download it from here (coming soon).

Now you can go to your Spark Installation Folder and run the following,

```sh

$ bin/spark-submit --class com.tuplejump.calliope.server.CalliopeServer2 /path/to/calliope-server-assembly-1.1.0-CTP-U2-SNAPSHOT.jar

```

Following this you can connect to the server using **beeline** as you would to the standard HiveThriftServer2. 

Please download the JDBC Driver Bundle from HERE (coming soon). The configure required to connect to the server with JDBC is same as to HiveThriftServer and is listed below.

You can also connect to it using **SquirrelSQL Client** and your Cassandra Tables wwill be listed under CASSANDRA_TABLES. Similarly you can connect to it using **Jasper**, **Birt**, etc.

For connecting to Tableau or any other tool that uses ODBC you will need to get the DataBricks ODBC driver.



