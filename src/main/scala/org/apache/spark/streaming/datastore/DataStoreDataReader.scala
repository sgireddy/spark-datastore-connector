package org.apache.spark.streaming.datastore


import com.google.datastore.v1._
import com.google.datastore.v1.client.DatastoreHelper
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.sources.v2.reader.DataReader
import scala.collection.JavaConverters._

/***
  * sgireddy 10/20/2018
  * Spark Structured Streaming Reader (spark.read) for google cloud datastore
  * @param dataSourceOptions DataSourceOptions, options provided through spark.read
  */
class DataStoreDataReader (dataSourceOptions: DataSourceOptions, pushedFilters: Array[Filter])
  extends DataReader[Row] {

  case class Value( json: String )

  private val options = dataSourceOptions.asMap().asScala
  val datastore = DatastoreHelper.getDatastoreFromEnv
  private val dataStoreKind = options.getOrElse("datastorekind", throw new Exception("Invalid dataStoreKind Kind"))

  var iterator: Iterator[Row] = null

  def next = {
    if (iterator == null) {

      val schema = Encoders.product[Value].schema
      val query = Query.newBuilder
      query.addKindBuilder.setName(dataStoreKind)

      val request = RunQueryRequest.newBuilder
      request.setQuery(query)
      val response = datastore.runQuery(request.build)
      iterator = response.getBatch.getEntityResultsList.asScala.toList
        .map(et => {
          val entity = et.getEntity
          val json = EntityJsonPrinter.print(entity)
          //Exclude Key
          //val props = mapper.toJson(mapper.readTree(jstr).get("properties"))
          //println(props)

          new GenericRowWithSchema(List(json).toArray[Any], schema)
        }).toIterator
    }
    iterator.hasNext
  }

  def get = {
    iterator.next()
  }
  def close() = Unit




}
