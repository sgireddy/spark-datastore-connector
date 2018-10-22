package org.apache.spark.streaming.datastore

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.collection.JavaConverters._

/***
  * sgireddy 10/20/2018
  * Spark Structured Streaming Reader (spark.read) for google cloud datastore
  * @param dataSourceOptions DataSourceOptions, options provided through spark.read
  */
class DataStoreBatchReader extends DataSourceV2  with ReadSupport with DataSourceRegister with Logging with Serializable  {

  def createReader(options: DataSourceOptions) = {
    new DataStoreSourceReader(options)
  }

  override def shortName(): String = "datastore-batch"

}

class DataStoreSourceReader(dataSourceOptions: DataSourceOptions)
  extends DataSourceReader with SupportsPushDownFilters  with Serializable {
  val options = dataSourceOptions.asMap().asScala.toMap
  var pushedFilters: Array[Filter] = Array[Filter]()

  def readSchema() = {
    val kind = options.getOrElse("datastorekind", throw new Exception("Invalid dataStoreKind Kind"))
    Utils.inferSchema(kind)
//    val columnNames = Array("value")
//    val structFields = columnNames.map(value ⇒ StructField(value, StringType))
//    StructType(structFields)
  }

  def pushFilters(filters: Array[Filter]) = {
    println("Filters " + filters.toList)
    pushedFilters = filters
    pushedFilters
  }

  def createDataReaderFactories = {

    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    factoryList.add(new DataStoreReaderFactory(options, pushedFilters, readSchema))
    factoryList
  }
}

class DataStoreReaderFactory(options: Map[String, String], pushedFilters: Array[Filter], schema: StructType)
  extends DataReaderFactory[Row] with Serializable {

  def createDataReader = new DataStoreDataReader(options, pushedFilters: Array[Filter], schema)
}
