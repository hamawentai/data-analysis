package com.lab.analysis

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object SalesRankAnalysis extends App {
  //  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val logger = Logger.getLogger(this.getClass)
  val hdfs = "hdfs://hadoop1:9000"
  val conf: SparkConf = new SparkConf().setAppName("spiderKeywords").setMaster("local[*]")
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val rddBase: RDD[String] = sc.textFile(hdfs + "/mock")

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
  }).sortBy(_._2._1.toLong, false)
  val result: Array[String] = rdd2.take(10).map(tuple => {
    tuple._1 + "\t" + tuple._2._1
  })
  sc.parallelize(result).saveAsTextFile(hdfs + "/result/out")
  result.foreach(println)
  sc.stop()
  spark.stop()
}
