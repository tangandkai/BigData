package com.tang.spark.df

import org.apache.spark.{SparkConf, SparkContext}
import it.nerdammer.spark.hbase._

/**
  * 读取hbase数据
  */
object SparkHBaseConnectorRead {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.hbase.host", "192.168.152.128")
    sparkConf.setMaster("local[2]").setAppName("SparkHBaseConnectorRead")
    val sc = new SparkContext(sparkConf)

    val hBaseRDD = sc.hbaseTable[(String, String, Int)]("student")
      .select("name", "gender","age")
      .inColumnFamily("info")

    hBaseRDD.foreach(println)
    sc.stop()
  }
}
