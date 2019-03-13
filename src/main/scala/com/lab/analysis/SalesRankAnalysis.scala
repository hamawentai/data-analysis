package com.lab.analysis

import java.util

import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  *
  */
object SalesRankAnalysis extends App {
  //  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)
  val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster(args(0))
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val hdfs = args(1)
  val rddBase: RDD[String] = sc.textFile(hdfs)

  val func1: String => (String, (String, String)) = (line: String) => {
    val fileds = line.split(",")
    val len = fileds.size
    if (len == 6) {
      val lag = fileds(0).split("--")
      val price = fileds(len - 3)
      val sales = fileds(len - 1)
      if (lag.size == 3) {
        (lag(0), (price, sales))
      } else {
        ("空", ("0", "0"))
      }
    } else {
      ("空", ("0", "0"))
    }
  }

  val rdd = rddBase.map(func1).filter(_._1 != "空")
  val rdd2 = rdd.reduceByKey((a, b) => {
    val totle1 = a._1.toDouble.toLong * a._2.toLong
    val totle2 = b._1.toDouble.toLong * b._2.toLong
    val totle = totle1 + totle2
    (totle.toString, "1")
  }).sortBy(_._2._1.toLong, ascending = false)
  //      tuple._1 + "\t" + tuple._2._1
  val result = rdd2.take(10).foreach(ele => {
    val client: MongoClient = new MongoClient("wx", 27017)
    val database: MongoDatabase = client.getDatabase("newdb")
    val collection: MongoCollection[Document] = database.getCollection("label_rank")
    val map = new util.HashMap[String, Object]()
    map.put("label", ele._1)
    map.put("value", ele._2._1)
    collection.insertOne(new Document(map))
  })
  sc.stop()
  spark.stop()
}
