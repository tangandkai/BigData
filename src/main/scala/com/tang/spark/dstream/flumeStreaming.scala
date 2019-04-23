package com.tang.spark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

object flumeStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("flumeStreaming")
    conf.setMaster("local[2]")

    StreamingExamples.setStreamingLogLevels()
    val ssc = new StreamingContext(conf,Seconds(5))
    //push
//    val flumeStreamPush = FlumeUtils.createStream(ssc, "0.0.0.0", 41414)
//
//    flumeStreamPush.map(x=>new String(x.event.getBody.array()).trim).
//      flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_).print()
    //pull
    val flumeStreamPull = FlumeUtils.createPollingStream(ssc,"192.168.152.128", 41414)
    flumeStreamPull.map(x=>new String(x.event.getBody.array()).trim).
      flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
