package org.apache.spark.sql

import com.datastax.driver.core.{KeyspaceMetadata, TableMetadata}
import com.tuplejump.calliope.sql.{CassandraAwareSQLContextFunctions, CassandraProperties, CassandraSchemaHelper}
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, LogicalPlan}

protected[sql] trait CassandraCatalog extends Catalog {
  protected def context: SQLContext with CassandraAwareSQLContextFunctions

  abstract override def lookupRelation(databaseName: Option[String], tableName: String, alias: Option[String]): LogicalPlan = {
    val cassandraProperties = CassandraProperties(context.sparkContext)
    import cassandraProperties._
    databaseName match {
      case Some(dbname) =>

        val metadata = CassandraSchemaHelper.getCassandraMetadata(cassandraHost, cassandraNativePort, cassandraUsername, cassandraPassword)
        if(metadata != null){
          metadata.getKeyspace(dbname) match {
            case ksmeta: KeyspaceMetadata =>
              ksmeta.getTable(tableName) match {
                case tableMeta: TableMetadata =>

                  val cschema = new SchemaRDD(context,
                    CassandraRelation(cassandraHost,
                      cassandraNativePort,
                      cassandraRpcPort,
                      dbname,
                      tableName,
                      context,
                      cassandraUsername,
                      cassandraPassword,
                      mayUseStargate,
                      Some(sparkContext.hadoopConfiguration)))

                  println(cschema.baseLogicalPlan.output)
                  val basePlan = cschema.baseLogicalPlan
                  val tableWithQualifers = Subquery(tableName, basePlan)

                  // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
                  // properly qualified with this alias.
                  alias.map(a => Subquery(a, tableWithQualifers)).getOrElse(basePlan)

                case null =>
                  super.lookupRelation(databaseName, tableName, alias)
              }
            case null =>
              super.lookupRelation(databaseName, tableName, alias)
          }
        }else{
          super.lookupRelation(databaseName, tableName, alias)
        }
      case None =>
        //We cannot fetch a table without the keyspace name in cassandra
        super.lookupRelation(databaseName, tableName, alias)
    }
  }
}