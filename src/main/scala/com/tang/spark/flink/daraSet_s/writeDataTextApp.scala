package com.tang.spark.flink.daraSet_s

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object writeDataTextApp {

  def main(args: Array[String]): Unit = {

    val input_path = "E:\\data\\flink\\people.txt"
    val output_path = "E:\\data\\flink\\sink\\02"
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input_path)
//    text.print()
//    text.writeAsText(output_path,WriteMode.OVERWRITE).setParallelism(1)

    import org.apache.flink.api.scala._
//    text.mapPartition(x=>x.flatMap(x1=>x1.split(" "))).print()
//    env.execute()
    val info = text.map(new RichMapFunction[String,String] {
      //定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        //注册计数器
        getRuntimeContext.addAccumulator("counter",counter)
      }
      override def map(value: String): String = {
        counter.add(1)
        value
      }
    })

    info.writeAsText(output_path)
    val jobResult = env.execute("writeDataTextApp")
    val count = jobResult.getAccumulatorResult[Long]("counter")
    println(count)
  }
}
