package com.tang.spark.imooc.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗
  */
object SparkStateFormatUtils {


  def parse(spark:SparkSession,input:String):RDD[String] ={

    //读取数据,清洗
    val logs = spark.sparkContext.textFile(input)
    val rdd = logs.map(line=>{
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3)+" "+splits(4)
      val url = splits(11).replaceAll("\"","")
      val traffic = splits(9)
      //      (ip,DateFormatUtils.parse(time),url,traffic)
      DateFormatUtils.parse(time)+"\t"+url+"\t"+traffic+"\t"+ip
    }).filter(line=>line.split("\t")(1)!="-")
    rdd
  }


//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().appName("SparkStateFormatJob").
//      master("local[2]").getOrCreate()
//
//    val inputpath = "hdfs://master:9000/user/hadoop/input/access.20161111.log"
//    val outputpath = "hdfs://master:9000/user/hadoop/input/access_1000"
//    //读取数据,清洗
//    val logs = spark.sparkContext.textFile(inputpath)
//    val rdd = logs.map(line=>{
//      val splits = line.split(" ")
//      val ip = splits(0)
//      val time = splits(3)+" "+splits(4)
//      val url = splits(11).replaceAll("\"","")
//      val traffic = splits(9)
////      (ip,DateFormatUtils.parse(time),url,traffic)
//      DateFormatUtils.parse(time)+"\t"+url+"\t"+traffic+"\t"+ip
//    })
//    rdd.take(10).foreach(println)
//
//    spark.stop()
//
//  }
}
