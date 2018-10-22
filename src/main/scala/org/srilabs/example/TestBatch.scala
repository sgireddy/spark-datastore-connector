package org.srilabs.example

import org.apache.spark.sql.SparkSession

object TestBatch extends App {

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  val kind = System.getenv("DATASTORE_KIND")

  val sampleStream = spark.read
    .format("org.apache.spark.streaming.datastore.DataStoreBatchReader")
    .option("datastorekind", kind)
    .load()

  sampleStream.printSchema()

  sampleStream.show()

}
