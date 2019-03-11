package com.lab.analysis

import java.util

import com.mongodb.{BasicDBObject, MongoClient}
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.bson.Document

import scala.collection.mutable

object RedistributionAnalysisChild {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val logger = Logger.getLogger(this.getClass)
    val client: MongoClient = new MongoClient("localhost", 27017)
    val database: MongoDatabase = client.getDatabase("newdb")
    val collection: MongoCollection[Document] = database.getCollection("redistribution")
    val hdfs = "hdfs://wx:9000"
    val conf: SparkConf = new SparkConf().setAppName("spiderKeywords").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val rddBase: RDD[String] = sc.textFile(hdfs + "/mock")
    val map = new mutable.HashMap[String, Int]()
    val list = List("黑龙江省", "福建省", "山西省", "江西省", "浙江省", "江苏省", "安徽省", "内蒙古自治区", "辽宁省", "河北省", "吉林省")
    var index = 0
    for (i <- list) {
      val docMap = new util.HashMap[String, Object]()
      docMap.put("province", i)
      docMap.put("detail", new util.ArrayList[Object]())
      val doc: Document = new Document(docMap)
      collection.insertOne(doc)
      map += (i -> index)
      index = index + 1
    }
    // 将一行数据拆分出所需数据 (address lag) sales*price
    val func1: String => ((String, String), Long) = (line: String) => {
      val fileds = line.split(",")
      val len = fileds.size
      if (len == 6) {
        val lag = fileds(0).split("--")
        var address = fileds(len - 4)
        if (address.contains("内蒙古")) {
          address = address.split("区")(0) + "区"
        } else {
          address = address.split("省")(0) + "省"
        }
        val price = fileds(len - 3).toDouble.toLong
        val sales = fileds(len - 1).toLong
        if (lag.size == 3) {
          ((address, lag(0)), price * sales)
        } else {
          (("空", "0"), 0l)
        }
      } else {
        (("空", "0"), 0l)
      }
    }
    val rdd: RDD[((String, String), Long)] = rddBase.map(func1).filter(_._1._1 != "空")
    sc.broadcast(map)
    val partition = ProvincePartition(list.size)(map)
    rdd.reduceByKey(_ + _).partitionBy(partition)
      .foreachPartition(_.toList.sortWith((a, b) => if (a._2 > b._2) true else false).foreach(ele => {
        val client: MongoClient = new MongoClient("wx", 27017)
        val database: MongoDatabase = client.getDatabase("newdb")
        val collection: MongoCollection[Document] = database.getCollection("redistribution")
        val query: BasicDBObject = new BasicDBObject()
        val map = new util.HashMap[String, Object]()
        map.put("label", ele._1._2.toString)
        map.put("value", ele._2.toString)
        val doc: Document = new Document("$push", new Document("detail", new Document(map)))
        query.put("province", ele._1._1)
        collection.updateMany(query, doc)
      }))

    sc.stop()
    spark.stop()
  }
}

case class ProvincePartition(num: Int)(map: mutable.Map[String, Int]) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key match {
    case (a: String, b: String) => {
      val no = map(a.toString)
      no
    }
    case _ => throw new Exception("key 值不对")
  }
}
