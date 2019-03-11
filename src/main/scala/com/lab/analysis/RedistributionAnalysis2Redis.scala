package com.lab.analysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

/** with redis
  * kind						 	              name																                       address		price comment  sales
  * 箱包皮具--功能箱包--休闲运动包|卡拉羊男女学生书包大容量防水休闲旅行包大高中学生书包背包双肩包 宝蓝|江苏省宿迁市|85.9|5189|6705
  * 区域分布分析
  * 区域：销量*价格
  */
object RedistributionAnalysis2Redis extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)
  val redisPool: JedisPool = RedisPool.getRedisPool
  val hdfs = "hdfs://wx:9000/mock"
  val conf: SparkConf = new SparkConf().setAppName("spiderKeywords").setMaster("local[*]")
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val rddBase: RDD[String] = sc.textFile(hdfs)
  // 将一行数据拆分出所需数据
  val func1: String => (String, String, String) = (line: String) => {
    val fileds = line.split(",")
    println(fileds.size, line)
    val len = fileds.size
    if (len < 6) {
      ("空", "0", "0")
    } else {
      val address = fileds(len - 4)
      val price = fileds(len - 3)
      val sales = fileds(len - 1)
      (address, price, sales)
    }
  }
  val rdd: Unit = rddBase.map(func1).foreachPartition(iterator => {
    val redis: Jedis = redisPool.getResource
    iterator.foreach(tuple => {
      //      println(tuple._1, tuple._2, tuple._3)
      var address: String = ""
      if (tuple._1 != "空") {
        if (tuple._1.contains("内蒙古")) {
          address = tuple._1.split("区")(0) + "区"
        } else {
          address = tuple._1.split("省")(0) + "省"
        }
        var price = 0.0
        try {
          price = tuple._2.toDouble
        } catch {
          case ex: Exception => {
            logger.error(tuple._1, tuple._2, tuple._3)
          }
        }
        val sales = tuple._3.toInt
        val is_exists = redis.exists(address)
        var sum = price
        if (is_exists == true) {
          sum = redis.get(address).toDouble + sum
          redis.set(address, sum.toString)
        } else {
          redis.set(address, sum.toString)
        }
      }
    })
    redis.close()
  })
  redisPool.close()
  sc.stop()
  spark.stop()
}
