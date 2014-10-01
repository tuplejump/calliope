package com.tuplejump.calliope

import java.nio.ByteBuffer

object Types {
  type CQLRowKeyMap = Map[CQLColumnName, CQLColumnValue]
  type CQLRowMap = Map[CQLColumnName, CQLColumnValue]
  type CQLRowValues = List[CQLColumnValue]
  type CQLKeyColumnName = String
  type CQLColumnName = String
  type CQLColumnValue = ByteBuffer

  type ThriftRowKey = ByteBuffer
  type ThriftColumnName = ByteBuffer
  type ThriftColumnValue = ByteBuffer
  type ThriftRowMap = Map[ThriftColumnName, ThriftColumnValue]
}
