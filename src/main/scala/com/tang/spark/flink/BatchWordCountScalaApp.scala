package com.tang.spark.flink

import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCountScalaApp {

  def main(args: Array[String]): Unit = {

    // 引入隐式转换
    import org.apache.flink.api.scala._
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("hdfs://master:9000/user/hadoop/wordcount.txt")
    text.print()
    val result = text.flatMap(x=>x.toLowerCase.split("\n")).filter(x=>x.nonEmpty).
      map(x=>(x,1)).groupBy(0)
    val s = result.first(1)
    s.print()
//    env.execute("BatchWordCountScalaApp")
  }
}
