package org.apache.spark.streaming.datastore

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.datastore.v1.client.{Datastore, DatastoreHelper}
import com.google.datastore.v1.{Entity, Query, RunQueryRequest}
import com.google.protobuf.Int32Value
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, GetStructField}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.util.Try

object Utils extends Serializable {

  @transient private lazy val datastore = DatastoreHelper.getDatastoreFromEnv
  @transient private lazy val mapper = getObjectMapper

  def inferSchema(kind: String): StructType = {

    val query = Query.newBuilder
    query.addKindBuilder.setName(kind)
    query.setLimit(Int32Value.newBuilder.setValue(1))
    val request = RunQueryRequest.newBuilder
    request.setQuery(query)
    val response = datastore.runQuery(request.build)
    val dsFields = response.getAllFields
    dsFields.asScala.foreach(println(_))

    val entity = response.getBatch.getEntityResultsList.asScala.toList(0).getEntity
    val json = EntityJsonPrinter.print(entity)
    val props = mapper.readTree(json).get("properties").fields().asScala
      .map(x => {
        val dataType = x.getValue.fields().asScala.map(x => x.getKey).toList.head
        (x.getKey, dataType)
      }).toMap

    val fields = props.flatMap(field => {
      val tpe  = getStructDataType(field._2)
      if (tpe.isDefined)
      Some(StructField(field._1, tpe.get))
      else None
    }).toArray
    val tpe = StructType(fields)
    println(tpe)
    tpe
  }

  def runQuery(kind: String, schema: StructType, offset: Int = -1, limit: Int = -1): Iterator[Row] = {
    val query = Query.newBuilder
    query.addKindBuilder.setName(kind)
    if(offset >= 0) query.setOffset(offset)
    if(limit > 0) query.setLimit(Int32Value.newBuilder.setValue(limit))
    val request = RunQueryRequest.newBuilder
    request.setQuery(query)
    val response = datastore.runQuery(request.build)
    response.getBatch.getEntityResultsList.asScala.toList
      .map(et => {
        Utils.entityToRow(et.getEntity, schema)
        //          val entity = et.getEntity
        //          val json = EntityJsonPrinter.print(entity)
        //          //Exclude Key
        //          //val props = mapper.toJson(mapper.readTree(jstr).get("properties"))
        //          new GenericRowWithSchema(List(json).toArray[Any], schema)
      }).toIterator
  }

  def getStructDataType(dsType: String): Option[DataType] = dsType match {
    case "nullValue" => Some(NullType) //??? this is fragile.. ignore and inspect another record?
    case "stringValue" => Some(StringType)
    case "timestampValue" => Some(StringType) // Some(TimestampType)
    case "integerValue" => Some(IntegerType)
    case "doubleValue" => Some(DoubleType)
    case "booleanValue" => Some(BooleanType)
    case "entityValue" => Some(StringType)
    case "arrayValue" => Some(StringType)
    case _ => None
  }

  def entityToRow(entity: Entity, schema: StructType): Row = {

    val props = entity.getPropertiesMap.asScala.toMap

    val values: List[Any] = schema.toList.map { field =>
      field.dataType match {
        case TimestampType =>
          if (props.get(field.name).isDefined)
            props(field.name).getTimestampValue
          else null
        case StringType =>
          if (props.get(field.name).isDefined)
            props(field.name).getStringValue
          else null
        case IntegerType =>
          if (props.get(field.name).isDefined)
            props(field.name).getIntegerValue
          else null
        case DoubleType =>
          if (props.get(field.name).isDefined)
            props(field.name).getDoubleValue
          else null
        case BooleanType =>
          if (props.get(field.name).isDefined)
            props(field.name).getBlobValue
          else null
        case _ => null
      }
    }
    val row = new GenericRowWithSchema(values.toArray, schema)
    println(row)
    row
  }

  def getObjectMapper = {
    import com.fasterxml.jackson.databind.SerializationFeature
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    objectMapper.findAndRegisterModules
    objectMapper
  }
}
