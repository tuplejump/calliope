---
layout: default
title: Transformers
---
# The dark side of the moon!
The data read from or written to cassandra is in form of ByteBuffers, so we need to provide ways to convert this to our application's domain and from our application's domain to ByteBuffers. Calliope needs some help with regard to reading the Cassandra data and build a RDD or to save an RDD back to a Cassandra table. This is done with the help of marshallers and unmarshallers. 

## How to provide these?
For convinience and to maintain code readability, the Marshaller and Unmarshaller parameters are implicit. Here is the list of transformers required by every calliope functions,

### Cql3 Reads

* **cql3Cassandra[T]** 
(Map[String, ByteBuffer], Map[String, ByteBuffer]) => T, where the first Map is a map of PrimaryKeys to their values and the second one is a Map of the rest of the values/columns.

* **cql3Cassandra[K, V]** 
Map[String, ByteBuffer] => K and another Map[String, ByteBuffer] => V. The first will give K from the Keys map and second will give V from the Values Map.

### Cql3 Writes
* **(r:RDD[U]).cql3SaveToCassandra**  
U => Map[String, ByteBuffer] and  U => List[ByteBuffer], the first will give the map of PrimaryKeys to their values for that record and the second one will give the list of values in order as in the Update Query.

### Thrift Reads

* **thriftCassandra[T]** 
(ByteBuffer, Map[ByteBuffer, ByteBuffer]) => T, where the first ByteBuffer is the cassandra row key and the second Map is the map of column names to their values.

* **thriftCassandra[K, V]**
ByteBuffer => K and Map[ByteBuffer, ByteBuffer] => V where the first one takes the row key ByteBuffer and gives us K, while the second on will conver the value map to domain object.

### Thrift Writes

* **(rdd:RDD[U]).thriftSaveToCassandra**
U => ByteBuffer and U => Map[ByteBuffer, ByteBuffer] used to convert the RDD entry to a key and column of maps.

## RichByteBuffer

The RichByteBuffer Object sypplies a number of implicits to conver the data coming from Cassandra to Scala objects. This allows you to work directly with simple datatypes like strings and numbers without worrying about the conversions.

Moreover it also helps you in writing your own transformers easily and we will see in the next section.

## Your own transformers

The best way to explain writing transformers, I feel, is to show some code,

* (Map[String, ByteBuffer], Map[String, ByteBuffer]) =>
    case class Employee(empid:String, deptid:String, name:String, age:Long, address:String)

```scala
import com.tuplejump.calliope.utils.RichByteBuffer._
implicit def CasMap2Employee(k:Map[String, ByteBuffer], v:Map[String, ByteBuffer]) =
    Employee(k("empid"), k("deptid"), v("name"), v("age"), v("address"))
```

As you see, we didn't have to worry about the difference in types of name being a String and age a Long. It's all the same to us, as the RichByteBuffer will kick in here and take care of the types.

* case class Employee(empid:String, deptid:String, name:String, age:Long, address:String) =>
    (Map[String, ByteBuffer], Map[String, ByteBuffer])

To go the other way round,

```scala
import com.tuplejump.calliope.utils.RichByteBuffer._
implicit def Employee2CasMap(e:Employee) =
    Map(
        "empid" -> e.empid,
        "deptid" -> e.deptid,
        "name" -> e.name,
        "age" -> e.age,
        "address" -> e.address
    )
```

It's that simple

## Why?

So the power of deciding how and what to transform lies with you!

In the coming releases we plan to automate the transformers for you, using Macros, but the transformers will always be available to help you use a custom serializer/deserilizer.



