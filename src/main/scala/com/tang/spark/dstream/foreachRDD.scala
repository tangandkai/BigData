package com.tang.spark.dstream

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 将统计结果写到mysql
  */
object foreachRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("foreachRDD")
    conf.setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(10))
    val lines = ssc.socketTextStream("192.168.152.128",9999)


    ssc.checkpoint("file:///data")
    val wordcount = lines.flatMap(_.split("\n")).map(x=>(x,1)).reduceByKey((a,b)=>a+b)
//    val wordcount = lines.flatMap(_.split("\n")).map(x=>(x,1)).
//      updateStateByKey[Int](updateFunction _)

    wordcount.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords=>{
        val connection = getConnection()
        var pstmt:PreparedStatement = null
        partitionOfRecords.foreach(record=>{
          val sql = "insert into wordcount values (?,?) ON DUPLICATE KEY UPDATE num=? + num"
          pstmt = connection.prepareStatement(sql)
          pstmt.setString(1,record._1)
          pstmt.setInt(2,record._2.toInt)
          pstmt.setInt(3,record._2.toInt)
          pstmt.execute()
        })
        pstmt.close()
        connection.close()
      })
    })


    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues: Seq[Int], oldValues: Option[Int]): Option[Int] = {
    val currentCount = currentValues.sum
    val oldCount = oldValues.getOrElse(0)
    Some(currentCount+oldCount)
  }

  /**
    * 获取数据库连接
    */
  def getConnection() ={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://192.168.152.128:3306/spark?user=root&password=123456&useSSL=false")
  }
}
