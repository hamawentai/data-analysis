package com.lab.analysis

import com.lab.PriceUtil
import com.lab.price.PriceDAO
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 价格购买量分析
  * 输出结果(价格,数量) 按照价格排序
  *
  * mysql jar
  * price
  */
object PriceAnalysis {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster(args(0))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    // 数据输入文件夹
    val INPUT_PATH = args(1)
    val rdd = sc.textFile(INPUT_PATH)
    val res = rdd.filter(line => {
      val len = line.split(",").size
      if (len == 6) {
        true
      } else {
        false
      }
    }).map(line => {
      val res: Array[String] = line.split(",")
      (res(3).toDouble, res(5).toInt) //返回(价格,数量)
    }).map(res => {
      val index = PriceUtil.getIndex(res._1)
      (index, res._2)
    }).reduceByKey(_ + _).sortBy(_._1).collect()

    res.foreach(res => {
      val price = PriceUtil.getPrice(res._1)
      PriceDAO.insertOrUpdate(price, res._2)
    })

    sc.stop()
  }
}

