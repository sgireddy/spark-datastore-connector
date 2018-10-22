package org.apache.spark.streaming.datastore

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.json4s.DefaultFormats

/***
  * sgireddy 10/21/2018
  * @param offset Offset
  * TODO: Datastore query offset says Int32 but that doesn't make sense (really only 2G records in a Kind?)
  */
case class DataStoreOffset (offset: Int) extends org.apache.spark.sql.sources.v2.reader.streaming.Offset {

  implicit val defaultFormats: DefaultFormats = DefaultFormats

  override val json = offset.toString

  def +(increment: Int): DataStoreOffset = new DataStoreOffset(offset + increment)
  def -(decrement: Int): DataStoreOffset = new DataStoreOffset(offset - decrement)
}

object DataStoreOffset {

  def apply(offset: Offset): DataStoreOffset = new DataStoreOffset(offset.json.toInt)

  def convert(offset: Offset): Option[DataStoreOffset] = offset match {
    case ds: DataStoreOffset => Some(ds)
    case serialized: SerializedOffset => Some(DataStoreOffset(serialized))
    case _ => None
  }
}