package com.tang.spark.flink.daraSet_s

import org.apache.flink.api.scala.ExecutionEnvironment

object WordCountExample {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.readCsvFileApp()
    import org.apache.flink.api.scala._
    val text = env.fromElements("Who's there?",
      "I think I hear them. Stand, ho! Who's there?")
    text.print()
    text.flatMap(x=>x.toLowerCase.trim.split(" ")).
      map(x=>(x,1)).groupBy(0).sum(1).print()


  }
}
