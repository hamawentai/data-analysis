package com.lab.analysis

import java.util.Properties

import com.lab.ansj.ScalaMysql
import com.lab.java.HotWordDAO
import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

/**
  * 词云分析
  *   ansj分词jar
  *   mysql jar
  */
object HotWordAnalysis {

  val INPUT = "/home/twl/Desktop/book/test.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("split")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext
    val split2Word  = (str: String) => {
      val list = new ArrayBuffer[(String, Int)]()
      val words = ToAnalysis.parse(str).getTerms
      val trems: List[Term] = JavaConverters.asScalaIteratorConverter(words.iterator()).asScala.toList
      trems.foreach(trem => if (trem.getNatureStr.contains("n")&&trem.getNatureStr!="null") list.append((trem.getName, 1)))
      list
    }
    val readRDD = sc.textFile(INPUT)
    readRDD.filter(_.split(",").size == 6)
      .map(_.split(",")(1))
      .flatMap(split2Word)
      .reduceByKey(_ + _)
      .map(line => {
        val word = line._1
        val number: Int = line._2
        HotWordDAO.getNumber(word,number)
      }).collect()

    sc.stop()
  }
}
