package com.tang.spark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object socketTextStream {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("socketTextStream")
    conf.setMaster("local[2]")

    StreamingExamples.setStreamingLogLevels()
    val ssc = new StreamingContext(conf,Seconds(10))

    val lines = ssc.socketTextStream("192.168.152.128",9999)
    val wordcount = lines.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    wordcount.print

    ssc.start()

    ssc.awaitTermination()

  }

}
