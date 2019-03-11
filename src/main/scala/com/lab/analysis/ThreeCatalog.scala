package com.lab.analysis

import com.lab.java.classify.ClassifyDAO
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * 存储三级分类(mysql)
  *
  * mysql jar
  */
object ThreeCatalog {
  val INPUT_PATH = "/home/twl/Desktop/book/test.txt"

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("three").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val rdd = sc.textFile(INPUT_PATH)

    val words = rdd.map(
      line => {
        val str = line.split("\t")(1)
        (str.split("--")(0),str.split("--")(1),str.split("--")(2))
      }
    )

    words.groupBy(_._1)
        .map(line=>{
          val list = line._2
          list.foreach(
            res=> {
              ClassifyDAO.insertSecond(res._1,res._2)
            }
          )
        }).collect()

    words.groupBy(_._2).map(line=>{
      val list: Iterable[(String, String, String)] = line._2
      list.foreach(
        res => {
          ClassifyDAO.insertThree(res._2,res._3)
        }
      )
    }).collect()

    sc.stop()
  }
}
