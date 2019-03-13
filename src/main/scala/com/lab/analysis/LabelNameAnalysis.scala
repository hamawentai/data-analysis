package com.lab.analysis

import java.util.Properties

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * 统计一级分类
  * spark sql
  * label
  */
object LabelNameAnalysis {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(args(0)).setAppName(this.getClass.getName)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    // 数据输入文件夹
    val INPUT_PATH = args(1)
    val rdd = sc.hadoopFile(INPUT_PATH, classOf[TextInputFormat],
      classOf[LongWritable], classOf[Text]).map(
      pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK"))

    val labelRdd = rdd
      .filter(line => {
        val len = line.split("\t").size
        if (len == 2) {
          val label = line.split("\t")(1)
          val labelSize = label.split("---")
          if (labelSize == 3) {
            true
          } else {
            false
          }
          true
        } else {
          false
        }
      })
      .map(line => {
        val labels = line.split("\t")(1)
        labels.split("--")(0)
      }).distinct()
    labelRdd.foreach(println)

    //通过StrutType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("label", StringType, true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD: RDD[Row] = labelRdd.map(p => {
      Row(p)
    })

    val lableData = spark.sqlContext.createDataFrame(rowRDD, schema)
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    lableData.write.mode("append").jdbc("jdbc:mysql://twl:3306/goods", "label", prop)
    sc.stop()
  }
}
