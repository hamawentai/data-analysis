package com.lab.analysis

import com.lab.HotWordDAO
import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

/**
  * 词云分析
  * ansj分词jar
  * mysql jar
  * hot_word
  */
object HotWordAnalysis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(args(0)).setAppName(this.getClass.getName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext
    val split2Word = (str: String) => {
      val list = new ArrayBuffer[(String, Int)]()
      val words = ToAnalysis.parse(str).getTerms
      val trems: List[Term] = JavaConverters.asScalaIteratorConverter(words.iterator()).asScala.toList
      trems.foreach(trem => if (trem.getNatureStr.contains("n") && trem.getNatureStr != "null") list.append((trem.getName, 1)))
      list
    }
    // 数据输入文件夹路径
    val INPUT = args(1)
    val readRDD = sc.textFile(INPUT)
    readRDD.filter(_.split(",").size == 6)
      .map(_.split(",")(1))
      .flatMap(split2Word)
      .reduceByKey(_ + _)
      .map(line => {
        val word = line._1
        val number: Int = line._2
        HotWordDAO.getNumber(word, number)
      }).collect()

    sc.stop()
  }
}
