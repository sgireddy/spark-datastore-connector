package org.srilabs.example

import java.util.Date
import com.google.datastore.v1.Entity
import com.google.datastore.v1.client.DatastoreHelper._
import scala.collection.JavaConverters._
object TestDataGenerator extends App {

  private val kind = "test"
  private val IDX_PROPERTY = "idx"
  private val DATE_PROPERTY = "date"
  private val MESSAGE_PROPERTY = "message"
  private lazy val datastore = getDatastoreFromEnv

  val projectId = System.getenv("DATASTORE_PROJECT_ID")

  (0 to 1000000).foreach(idx => {
    addEntity(idx)
    Thread.sleep(1000)
  })

  def addEntity(idx: Int): Unit = {
    val entity = Entity.newBuilder()
    val seq: Seq[String] = Seq(kind) //, idx.toString) //, idx)
    val key = makeKey(seq: _*)
    entity.setKey(key)
    entity.getMutableProperties.put(IDX_PROPERTY, makeValue(idx).build)
    entity.getMutableProperties.put(MESSAGE_PROPERTY, makeValue("Hello").build)
    entity.getMutableProperties.put(DATE_PROPERTY, makeValue(new Date()).build)
    val greetingKey = insert(entity.build)
    System.out.println("key: " + greetingKey)
  }

  import com.google.datastore.v1.CommitRequest
  import com.google.datastore.v1.Mutation
  import com.google.datastore.v1.client.DatastoreException

  @throws[DatastoreException]
  private def insert(entity: Entity) = {
    val req = CommitRequest.newBuilder
      .addMutations(Mutation.newBuilder.setInsert(entity))
      .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
      .build
    datastore.commit(req).getMutationResults(0).getKey
  }
}
