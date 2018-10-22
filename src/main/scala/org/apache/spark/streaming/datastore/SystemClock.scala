package org.apache.spark.streaming.datastore

class SystemClock {

  val minPollTime = 25L

  /**
    * @return the same time (milliseconds since the epoch)
    *         as is reported by `System.currentTimeMillis()`
    */
  def getTimeMillis(): Long = System.currentTimeMillis()

}
