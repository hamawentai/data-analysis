package com.lab.analysis

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * 将标签存入数据库
  */
object LabelAnalysis {

  val INPUT_PATH = "/home/twl/Desktop/book/kind.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("labelName").set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val rdd = sc.textFile(INPUT_PATH)

    val resRdd = rdd.filter(line => {
      val res = line.split(",")
      if (res.size == 3) {
        true
      } else {
        false
      }
    }).map(_.split(",")(1))

    //通过StrutType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("labelName",StringType,true)
      )
    )

    //将RDD映射到rowRDD
    val rowRDD = resRdd.map(p=>{Row(p)})

    val lableData = spark.sqlContext.createDataFrame(rowRDD,schema)
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")

    lableData.write.mode("append").jdbc("jdbc:mysql://twl:3306/goods", "catalog",prop)
    sc.stop()
  }
}
