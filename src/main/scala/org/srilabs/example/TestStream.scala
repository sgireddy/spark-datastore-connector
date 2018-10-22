package org.srilabs.example

import org.apache.spark.sql.SparkSession

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
    .writeStream
    .format("console")
    .start()

}
