package org.apache.spark.streaming.datastore


import com.google.datastore.v1._
import com.google.datastore.v1.client.DatastoreHelper
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

  case class Value( json: String )

  val datastore = DatastoreHelper.getDatastoreFromEnv
  private val dataStoreKind = options.getOrElse("datastorekind", throw new Exception("Invalid dataStoreKind Kind"))

  var iterator: Iterator[Row] = null

  def next = {
    if (iterator == null) {

      //val schema = Encoders.product[Value].schema
      val query = Query.newBuilder
      query.addKindBuilder.setName(dataStoreKind)

      val request = RunQueryRequest.newBuilder
      request.setQuery(query)
      val response = datastore.runQuery(request.build)
      val list = response.getBatch.getEntityResultsList.asScala.toList
        .map(et => {
          Utils.entityToRow(et.getEntity, schema)
//          val entity = et.getEntity
//          val json = EntityJsonPrinter.print(entity)
//          //Exclude Key
//          //val props = mapper.toJson(mapper.readTree(jstr).get("properties"))
//          new GenericRowWithSchema(List(json).toArray[Any], schema)
        })
      iterator = list.toIterator
    }
    iterator.hasNext
  }

  def get = {
    iterator.next()
  }
  def close() = Unit




}
