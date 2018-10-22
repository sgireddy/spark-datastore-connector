package org.apache.spark.streaming.datastore

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/***
  * sgireddy 10/20/2018
  * Spark Structured Streaming Reader (spark.read) for google cloud datastore
  * @param dataSourceOptions DataSourceOptions, options provided through spark.read
  */
class DataStoreBatchReader extends DataSourceV2  with ReadSupport with DataSourceRegister with Logging  {

  def createReader(options: DataSourceOptions) = {
    new DataStoreSourceReader(options)
  }

  override def shortName(): String = "datastore-batch"

}

class DataStoreSourceReader(dataSourceOptions: DataSourceOptions) extends DataSourceReader with SupportsPushDownFilters {

  var pushedFilters: Array[Filter] = Array[Filter]()
  def readSchema() = {
    val columnNames = Array("value")
    val structFields = columnNames.map(value ⇒ StructField(value, StringType))
    StructType(structFields)
  }

  def pushFilters(filters: Array[Filter]) = {
    println("Filters " + filters.toList)
    pushedFilters = filters
    pushedFilters
  }

  def createDataReaderFactories = {

    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    factoryList.add(new DataStoreReaderFactory(dataSourceOptions, pushedFilters))
    factoryList
  }
}

class DataStoreReaderFactory(dataSourceOptions: DataSourceOptions, pushedFilters: Array[Filter])
  extends DataReaderFactory[Row] {

  def createDataReader = new DataStoreDataReader(dataSourceOptions, pushedFilters: Array[Filter])
}
