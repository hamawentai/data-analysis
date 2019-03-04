package com.lab.ansj

import java.sql
import java.sql.{DriverManager, ResultSet}

import com.mysql.jdbc.Connection

object ScalaMysql {
 
  def main(args: Array[String]) {
    print(getNumber("口味"))
  }

  def getNumber(word:String) = {

    var num = 0

    val connection: sql.Connection = getConnection()

    try {
      val prep = connection.prepareStatement("select * from hot_word where word = ?")
      prep.setString(1, word)
      val result: ResultSet = prep.executeQuery

      while (result.next()) {
        num = result.getInt("number")
      }
    } finally {
      connection.close()
    }
    num
  }

  def getConnection()= {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/goods"
    val username = "root"
    val password = "123456"

    Class.forName(driver)
    val connection: sql.Connection = DriverManager.getConnection(url, username, password)
    connection
  }
}
