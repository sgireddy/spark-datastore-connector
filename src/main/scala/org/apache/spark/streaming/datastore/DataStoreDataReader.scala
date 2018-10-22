package org.apache.spark.streaming.datastore

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

/***
  * sgireddy 10/20/2018
  * Spark Structured Streaming Reader (spark.read) for google cloud datastore
  * @param dataSourceOptions DataSourceOptions, options provided through spark.read
  */
class DataStoreDataReader (options: Map[String, String], pushedFilters: Array[Filter], schema: StructType)
  extends DataReader[Row]  with Serializable {

  private val dataStoreKind = options.getOrElse("datastorekind", throw new Exception("Invalid dataStoreKind Kind"))

  var iterator: Iterator[Row] = null

  def next: Boolean = {
    if (iterator == null) {
      iterator = Utils.runQuery(dataStoreKind, schema)
    }
    iterator.hasNext
  }

  def get: Row = {
    iterator.next()
  }
  def close(): Unit = Unit

}
