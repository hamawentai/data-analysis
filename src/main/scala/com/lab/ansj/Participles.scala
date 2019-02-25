package com.lab.ansj

import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

object Participles {

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
    readRDD
      .filter(_.split(",").size == 6)
      .map(_.split(",")(1))
      .flatMap(split2Word).reduceByKey(_ + _).sortBy(_._2).collect().foreach(println)
  }
}
