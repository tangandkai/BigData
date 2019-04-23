package com.tang.spark.imooc.utils

import java.sql.{Connection, DriverManager, PreparedStatement}




object MysqlUtils {

    /**
      * 获取数据库连接
      */
    def getConnection() ={
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://master:3306/spark?user=root&password=123456&useSSL=false")
    }

  /**
    * 释放数据库资源
    * @param connection
    * @param psmt
    */
  def release(connection: Connection,psmt:PreparedStatement): Unit ={
    try{
      if (psmt != null){
        psmt.close()
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      if (connection != null){
        connection.close()
      }
    }
  }
}
