package org.srilabs.example

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

object TestStream extends App {

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  val kind = System.getenv("DATASTORE_KIND")

  val sampleStream = spark.readStream
    .format("org.apache.spark.streaming.datastore.DataStoreStreamReader")
    .option("datastorekind", kind)
    .option("batchSize", "25")
    .load()

  sampleStream.printSchema()

  val query: StreamingQuery = sampleStream
    .writeStream
    .outputMode("append")
    .queryName("sample")
    .format("memory")
    .start()

  for(_ <- 0 to 10) {
    Thread.sleep(TimeUnit.SECONDS.toMillis(2))
    spark.sql("select * from sample").show()
  }

  query.stop()

}
