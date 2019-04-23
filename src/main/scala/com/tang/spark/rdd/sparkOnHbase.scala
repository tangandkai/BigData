package com.tang.spark.rdd

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

/**
  * 使用hbaseContext快速操作hbase
  * 可以在regionserve上执行executor
  * 把每一个spark执行器想象成一个多线程客户机应用程序
  */
object sparkOnHbase {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[2]", "sparkOnHbase")

    //hbase 配置
    val config =  HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum","master:2181")
    config.set("hbase.zookeeper.property.clientPort", "2181")

    val tableName = "student"
    //配置输入文件
    val input = "hdfs://master:9000/user/hadoop/input/student.txt"
    val rdd = sc.textFile(input).map(_.split("\t"))

    rdd.foreach(x=>println("the first:"+x(0)+"the second:"+x(1)))
//    val hbaseContext = new HBaseContext(sc, config)
//    hbaseContext.bulkPut[Array[String]](rdd,tableName,
//      (putRecord) => {
//        val put = new Put(Bytes.toBytes(putRecord(0)))
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(putRecord(1)))
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(putRecord(2)))
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(putRecord(3)))
//        put
//      })
  }

}
