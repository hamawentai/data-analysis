package com.lab.ansj

import java.util.Properties

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySQL-Demo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //通过并行化创建RDD
    val personRDD = sc.parallelize(Array("14 tom", "15 jerry", "16 kitty")).map(_.split(" "))
    //通过StrutType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("label",StringType,true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(1).trim))
    rowRDD.foreach(println)
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD,schema)
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    //将数据追加到数据库
    personDataFrame.write.mode("append").jdbc("jdbc:mysql://localhost:3306/goods",
      "label",prop)
    //停止SparkContext
    sc.stop()
  }
}