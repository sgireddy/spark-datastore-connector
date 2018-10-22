package org.apache.spark.streaming.datastore

import com.google.datastore.v1.Entity
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.util.JsonFormat.TypeRegistry

/***
  * sgireddy 10/20/2018
  * Converts an Entity to a JSON String.
  */
object EntityJsonPrinter {
  val typeRegistry = TypeRegistry.newBuilder.add(Entity.getDescriptor).build
  val jsonPrinter = JsonFormat.printer.usingTypeRegistry(typeRegistry).omittingInsignificantWhitespace

  @throws[InvalidProtocolBufferException]
  def print(entity: Entity): String = jsonPrinter.print(entity)
}
