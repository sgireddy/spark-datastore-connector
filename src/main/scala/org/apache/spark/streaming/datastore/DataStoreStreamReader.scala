package org.apache.spark.streaming.datastore

import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types.StructType

/***
  * sgireddy 10/21/2018
  * Spark Structured Streaming MicroBatchReader for google cloud datastore
  * @param dataSourceOptions DataSourceOptions, options provided through spark.readStream
  */
class DataStoreStreamReader extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister with Logging{

  override def createMicroBatchReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): MicroBatchReader = {
    new DataStoreMicroBatchReader(options)
  }

  override def shortName(): String = "datastore-stream"
}
