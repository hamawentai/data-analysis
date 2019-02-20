package com.lab.analysis

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
  * with hdfs
  */

object RedistributionAnalysiy2HDFS extends App {
  val logger = Logger.getLogger(this.getClass)
  val hdfs = "hdfs://hadoop1:9000"
  val conf: SparkConf = new SparkConf().setAppName("spiderKeywords").setMaster("local[*]")
  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val rddBase: RDD[String] = sc.textFile(hdfs + "/mock")
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
  }).map(tuple => {
    tuple._1 + "\t" + tuple._2._1
  }).saveAsTextFile(hdfs + "/result/partitionout")
  sc.stop()
  spark.stop()
}
