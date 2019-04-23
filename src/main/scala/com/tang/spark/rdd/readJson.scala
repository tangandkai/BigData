package com.tang.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object readJson {

  def main(args: Array[String]): Unit = {

    val inputFile = "hdfs://192.168.152.128:9000/user/hadoop/input/people.json"
    val conf = new SparkConf().setMaster("local[1]").setAppName("readJson")
    val sc = new SparkContext(conf)
    val jsonStr = sc.textFile(inputFile)
    val result = jsonStr.map(x=>JSON.parseFull(x))
    result.foreach(x=> x match {
      case Some(map: Map[String,Any])=> println(map)
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    })

    sc.stop()
  }

}
