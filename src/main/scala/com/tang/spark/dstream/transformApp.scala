package com.tang.spark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object transformApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("foreachRDD")
    conf.setMaster("local[2]")


    val ssc = new StreamingContext(conf,Seconds(10))
    val blacks = List("zhangsan","wangwu")
    val blackRDD = ssc.sparkContext.parallelize(blacks).map(x=>(x,true))
    val lines = ssc.socketTextStream("192.168.152.128",9999)
    val result = lines.map(x=>(x.split(",")(1),x)).transform(rdd=>{
      rdd.leftOuterJoin(blackRDD).filter(x=>x._2._2.getOrElse(false)!=true).map(x=>x._2._1)
    })

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
