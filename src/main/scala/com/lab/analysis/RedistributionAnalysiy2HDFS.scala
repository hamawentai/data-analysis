package com.lab.analysis

import java.util

import com.mongodb.MongoClient
import com.mongodb.client.{MongoCollection, MongoDatabase}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * with hdfs
  */

object RedistributionAnalysiy2HDFS extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)
  val hdfs = args(1)
  val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster(args(0))
  val inputFile = hdfs
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val rddBase: RDD[String] = sc.textFile(inputFile)
  // 将一行数据拆分出所需数据
  val func1: String => (String, (String, String)) = (line: String) => {
    val fileds = line.split(",")
    val len = fileds.size
    if (len < 6) {
      ("空", ("0", "0"))
    } else {
      var address = fileds(len - 4)
      if (address.contains("内蒙古")) {
        address = address.split("区")(0) + "区"
      } else {
        address = address.split("省")(0) + "省"
      }
      val price = fileds(len - 3)
      val sales = fileds(len - 1)
      (address, (price, sales))
    }
  }

  val rdd = rddBase.map(func1).filter(_._1 != "空")
  rdd.reduceByKey((a, b) => {
    val totle1 = a._1.toDouble.toLong * a._2.toLong
    val totle2 = b._1.toDouble.toLong * b._2.toLong
    val totle = totle1 + totle2
    (totle.toString, "1")
  }).foreachPartition(iterator => {
    iterator.toList.foreach(ele => {
      println(ele._1, ele._2)
      val client: MongoClient = new MongoClient("wx", 27017)
      val database: MongoDatabase = client.getDatabase("newdb")
      val collection: MongoCollection[Document] = database.getCollection("province_rank")
      val map = new util.HashMap[String, Object]()
      map.put("province", ele._1)
      map.put("value", ele._2._1)
      collection.insertOne(new Document(map))
    })
  })
  sc.stop()
  spark.stop()
}
