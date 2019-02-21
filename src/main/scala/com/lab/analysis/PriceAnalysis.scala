package com.lab.analysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

/**
  * 价格购买量分析
  * 输出结果(价格,数量) 按照价格排序
  */
object PriceAnalysis {

  val INPUT_PATH = "/home/twl/Desktop/book/test.txt"
  val OUTPUT_PATH = "/home/twl/Desktop/book/result"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("price").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val rdd = sc.textFile(INPUT_PATH)
    val res = rdd.filter(line=>{
      val len = line.split(",").size
      if (len == 6) {
        true
      } else {
        false
      }
    })
      .map(line => {
      val res: Array[String] = line.split(",")
      (res(3).toDouble,res(5).toInt) //返回(价格,数量)
    }).reduceByKey(_+_)

    res.sortByKey()
      .map(res=>{
        res._1+","+res._2
      }).saveAsTextFile(OUTPUT_PATH)

    sc.stop()
  }
}

