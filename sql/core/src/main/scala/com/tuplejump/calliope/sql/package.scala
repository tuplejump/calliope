package com.tuplejump.calliope

import org.apache.spark.sql.{CassandraSchemaRDD, SchemaRDD}

package object sql {
  implicit def SchemaRDD2CassandraSchemaRDD(table: SchemaRDD) = new CassandraSchemaRDD(table)
}
