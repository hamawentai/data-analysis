import java.util

import com.mongodb.client.{FindIterable, MongoCollection, MongoCursor, MongoDatabase}
import com.mongodb.{BasicDBObject, MongoClient}
import org.bson.Document

/**
  * @author weixun
  * @data 19-3-10 下午4:55
  */
object MongodbConnection extends App {

  println("come")
  val client: MongoClient = new MongoClient("localhost", 27017)
  val database: MongoDatabase = client.getDatabase("newdb")
  val collection: MongoCollection[Document] = database.getCollection("redistribution")
  val document: FindIterable[Document] = collection.find(new Document("province", "吉林省"))
  val iterator: MongoCursor[Document] = document.iterator()
  while (iterator.hasNext) {
//    println(iterator.next())
    println(iterator.next().get("detail"))
  }
  val query: BasicDBObject = new BasicDBObject()
  val map = new util.HashMap[String, Object]()
  map.put("name", "file1")
  map.put("type", "file")
  query.put("username", "tom")
  val doc: Document = new Document("$push", new Document("result.0.list", new Document(map)))
  collection.updateMany(query, doc)
}
