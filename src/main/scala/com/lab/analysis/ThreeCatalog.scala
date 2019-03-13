package com.lab.analysis

import com.lab.classify.ClassifyDAO
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 存储三级分类(mysql)
  *
  * mysql jar
  */
object ThreeCatalog {

  def main(args: Array[String]): Unit = {


    println("------------",args(0), args(1))
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster(args(0))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val INPUT_PATH = args(1)
    val rdd = sc.textFile(INPUT_PATH)
    val words = rdd.map(
      line => {
        val str = line.split("\t")(1)
        (str.split("--")(0), str.split("--")(1), str.split("--")(2))
      }
    )
    words.groupBy(_._1)
      .map(line => {
        val list = line._2
        list.foreach(
          res => {
            ClassifyDAO.insertSecond(res._1, res._2)
          }
        )
      }).collect()

    words.groupBy(_._2).map(line => {
      val list: Iterable[(String, String, String)] = line._2
      list.foreach(
        res => {
          ClassifyDAO.insertThree(res._2, res._3)
        }
      )
    }).collect()

    sc.stop()
  }
}
