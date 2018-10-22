package org.apache.spark.streaming.datastore

import java.util.Optional

import com.google.datastore.v1._
import com.google.datastore.v1.client.DatastoreHelper
import com.google.protobuf.{Int32Value, Int32ValueOrBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/***
  * sgireddy 10/21/2018
  * Spark Structured Streaming MicroBatchReader for google cloud datastore
  * @param dataSourceOptions DataSourceOptions, options provided through spark.readStream
  */
class DataStoreMicroBatchReader(dataSourceOptions: DataSourceOptions) extends MicroBatchReader with Serializable {

  case class Value( json: String)

  private val options = dataSourceOptions.asMap().asScala
  private val initialOffset = options.getOrElse("initialoffset", "0").toInt
  private val dataStoreKind = options.getOrElse("datastorekind", throw new Exception("Invalid dataStoreKind Kind"))
  // private val offsetColumnName = options.getOrElse("offsetcolumnname", throw new Exception("Invalid dataStoreKind Kind"))

  private val dataList: ListBuffer[String] = new ListBuffer[String]() //TODO: to entity???

  private var startOffset: DataStoreOffset = DataStoreOffset(initialOffset)
  private var endOffset: DataStoreOffset = DataStoreOffset(-1)

  private var currentOffset: DataStoreOffset = DataStoreOffset(-1)
  private var lastReturnedOffset: DataStoreOffset = DataStoreOffset(-2)
  private var lastOffsetCommitted : DataStoreOffset = DataStoreOffset(-1)
  private val batchSize = options.getOrElse("batchSize", "50").toInt
  private val datastore = DatastoreHelper.getDatastoreFromEnv

  private val NO_DATA_OFFSET = DataStoreOffset(-1)
  private var stopped: Boolean = false

  private var worker:Thread = null

  private var incomingEventCounter = 0

  // kick off a thread to start receiving the events
  initialize()

  private def initialize(): Unit = synchronized {


    worker = new Thread("DataStore Worker") {
      setDaemon(true)
      override def run() {
        receive()
      }
    }
    worker.start()
  }

  private def receive(): Unit = {

    val query = Query.newBuilder
    query.addKindBuilder.setName(dataStoreKind)
    query.setOffset(startOffset.offset + 1)
    query.setLimit(Int32Value.newBuilder.setValue(batchSize))
    val request = RunQueryRequest.newBuilder

    request.setQuery(query)
    val response = datastore.runQuery(request.build)

    dataList.clear() // Is it being cleared elsewhere? does it cause issues?

    dataList ++= response.getBatch.getEntityResultsList.asScala.toList
      .map(et => {
        val entity = et.getEntity
        EntityJsonPrinter.print(entity)
      })
  }

  override def readSchema(): StructType = {
    StructType(
      //StructField("timestamp", TimestampType, false) ::
      StructField("value", StringType, false) :: Nil)  //json representation of entity TODO: Infer Schema
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    synchronized {
      val startOrdinal = startOffset.offset.toInt + 1
      val endOrdinal = endOffset.offset.toInt + 1

      val newBlocks = synchronized {
        val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
        val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
        assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
        dataList.slice(sliceStart, sliceEnd)
      }

      newBlocks.grouped(batchSize).map { block =>
        new DataStoreStreamBatchTask(block).asInstanceOf[DataReaderFactory[Row]]
      }.toList.asJava
    }
  }

  override def deserializeOffset(json: String): Offset = DataStoreOffset(json.toInt)

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    this.startOffset = start.orElse(NO_DATA_OFFSET).asInstanceOf[DataStoreOffset]
    this.endOffset = end.orElse(currentOffset).asInstanceOf[DataStoreOffset]
  }

  override def getStartOffset: Offset = {
    if (startOffset.offset == -1) {
      throw new IllegalStateException("startOffset is -1")
    }
    startOffset
  }

  override def getEndOffset: Offset = {
    if (endOffset.offset == -1) {
      currentOffset
    } else {
      if (lastReturnedOffset.offset < endOffset.offset) {
        lastReturnedOffset = endOffset
      }
      endOffset
    }
  }

  override def commit(end: Offset): Unit = {
    val newOffset = DataStoreOffset.convert(end).getOrElse(
      sys.error(s"SampleStreamMicroBatchReader.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )
    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt
    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }
    dataList.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  override def stop(): Unit = stopped = true

}

class DataStoreStreamBatchTask(dataList: ListBuffer[String])
  extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new DataStoreStreamBatchReader(dataList)
}

class DataStoreStreamBatchReader(dataList: ListBuffer[String]) extends DataReader[Row] {
  private var currentIdx = -1

  override def next(): Boolean = {
    currentIdx += 1
    currentIdx < dataList.size
  }

  override def get(): Row = Row(dataList(currentIdx), s"currentIdx = $currentIdx")

  override def close(): Unit = ()
}


