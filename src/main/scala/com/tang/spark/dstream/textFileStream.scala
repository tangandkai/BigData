package com.tang.spark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object textFileStream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("textFileStream")
    conf.setMaster("local[2]")

    val ssc = new StreamingContext(conf,Seconds(10))
    val lines = ssc.textFileStream("hdfs://192.168.152.128:9000/user/hadoop/wordcount.txt")

    ssc.checkpoint("file:///data")
//    val wordcount = lines.flatMap(_.split("\n")).map(x=>(x,1)).reduceByKey((a,b)=>a+b)
    val wordcount = lines.flatMap(_.split("\n")).map(x=>(x,1)).
      updateStateByKey[Int](updateFunction _)

    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues: Seq[Int], oldValues: Option[Int]): Option[Int] = {
    val currentCount = currentValues.sum
    val oldCount = oldValues.getOrElse(0)
    Some(currentCount+oldCount)
  }

}
