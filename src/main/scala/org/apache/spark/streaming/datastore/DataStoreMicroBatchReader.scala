package org.apache.spark.streaming.datastore

import java.util.Optional
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
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
  private val dataList: ListBuffer[Row] = new ListBuffer[Row]() //TODO: to entity???
  private var startOffset: DataStoreOffset = DataStoreOffset(initialOffset)
  private var endOffset: DataStoreOffset = DataStoreOffset(-1)
  private var currentOffset: DataStoreOffset = DataStoreOffset(-1)
  private var lastReturnedOffset: DataStoreOffset = DataStoreOffset(-2)
  private var lastOffsetCommitted : DataStoreOffset = DataStoreOffset(-1)
  private val batchSize = options.getOrElse("batchSize", "50").toInt
  private val queueSize = options.getOrElse("queueSize", "512").toInt
  private val producerRate = options.getOrElse("producerRate", "256").toInt
  private val NO_DATA_OFFSET = DataStoreOffset(-1)
  private var stopped: Boolean = false
  private var incomingEventCounter = 0
  private var producer: Thread = _
  private var consumer: Thread = _
  private val dataQueue: BlockingQueue[Row] = new ArrayBlockingQueue(queueSize)

  // kick off a thread to start receiving the events
  initialize()

  private def initialize(): Unit = synchronized {

    producer = new Thread("Data Producer") {
      setDaemon(true)
      override def run() {
        var counter: Long = 0
        while(!stopped) {
          Utils.runQuery(dataStoreKind, readSchema(), startOffset.offset + 1, batchSize )
            .foreach(row => {
              dataQueue.put(row)
              counter += 1
            })
          //Thread.sleep(producerRate)
        }
      }
    }
    producer.start()

    consumer = new Thread("Data Consumer") {
      setDaemon(true)
      override def run() {
        while (!stopped) {
          val id = dataQueue.poll(100, TimeUnit.MILLISECONDS)
          if (id != null.asInstanceOf[Row]) {
            dataList.append(id)
            currentOffset = currentOffset + 1
          }
        }
      }
    }
    consumer.start()
  }

  override def readSchema(): StructType = Utils.inferSchema(dataStoreKind)

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    synchronized {
      val startOrdinal = startOffset.offset.toInt + 1
      val endOrdinal = endOffset.offset.toInt + 1

      val newBlocks = synchronized {
        val sliceStart = startOrdinal - lastOffsetCommitted.offset - 1
        val sliceEnd = endOrdinal - lastOffsetCommitted.offset - 1
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

class DataStoreStreamBatchTask(dataList: ListBuffer[Row])
  extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new DataStoreStreamBatchReader(dataList)
}

class DataStoreStreamBatchReader(dataList: ListBuffer[Row]) extends DataReader[Row] {
  private var currentIdx = -1

  override def next(): Boolean = {
    currentIdx += 1
    currentIdx < dataList.size
  }

  override def get(): Row = Row(dataList(currentIdx), s"currentIdx = $currentIdx")

  override def close(): Unit = ()
}


