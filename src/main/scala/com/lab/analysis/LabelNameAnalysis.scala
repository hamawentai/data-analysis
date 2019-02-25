package com.lab.analysis

import com.lab.analysis.PriceAnalysis.INPUT_PATH
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * 统计一级分类
  */
object LabelNameAnalysis {
  val INPUT_PATH = "/home/twl/Desktop/book/label.txt"
  val OUTPUT_PATH = "/home/twl/Desktop/book/result"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("label")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val rdd = sc.textFile(INPUT_PATH)
    rdd.map(line=>{
      val labels = line.split("\t")(1)
      labels.split("--")(0)
    }).distinct().saveAsTextFile(OUTPUT_PATH)
    sc.stop()
  }
}
