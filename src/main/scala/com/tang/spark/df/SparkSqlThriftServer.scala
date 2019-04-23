package com.tang.spark.df

import java.sql.DriverManager

/**
  * 通过jdbc方式连接
  */
object SparkSqlThriftServer {

  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://master:10000","root","")
    val pstmt = conn.prepareStatement("select * from spark.user_info")
    val result = pstmt.executeQuery()
    while (result.next()){

      System.out.println("id: "+result.getString("id")+"\n"+
      "name: "+result.getString("name")+"\n"+
      "address: "+result.getObject("address")+"\n"+
      "email: "+result.getString("email")+"\n"+
      "ph_num: "+result.getString("ph_num"))
    }
    result.close()
    pstmt.close()
    conn.close()
  }


}
