package com.tang.spark.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWindowBasioc {

  case class WC(word:String,count:Int)

  def main(args: Array[String]): Unit = {

    val host = "master"
    val port = 9999
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.socketTextStream(host,port)
    import org.apache.flink.api.scala._
//    source.flatMap(x=>x.toLowerCase.split(" ")).
//      map(x=>(x,1)).keyBy(0).timeWindow(Time.seconds(10),Time.seconds(2)).
//      sum(1).print().setParallelism(1)

//    source.flatMap(x=>x.toLowerCase.split(" ")).
//      map(x=>WC(x,1)).keyBy("word").timeWindow(Time.seconds(10),
//      Time.seconds(2)).
//      sum("count").print().setParallelism(1)

    source.flatMap(x=>x.toLowerCase.split(" ")).
      map(x=>WC(x,1)).keyBy(x=>x.word).timeWindow(Time.seconds(10),
      Time.seconds(2)).
      sum("count").print().setParallelism(1)
    env.execute("StreamingWindowBasioc")
  }
}
