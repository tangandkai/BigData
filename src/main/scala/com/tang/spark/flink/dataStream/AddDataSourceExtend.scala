package com.tang.spark.flink.dataStream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object AddDataSourceExtend {



  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    nonParallelSourceFunction(env)
    parallelSourceFunc(env)

    env.execute("AddDataSourceExtend")
  }

  def nonParallelSourceFunction(Env:StreamExecutionEnvironment): Unit ={
    import org.apache.flink.api.scala._
    val data = Env.addSource(new CustNonParallelSourceFunc).setParallelism(1)
    data.print().setParallelism(1)
  }

  def parallelSourceFunc(Env:StreamExecutionEnvironment): Unit ={
    import org.apache.flink.api.scala._
    val data = Env.addSource(new CusParallelSourceFunc).setParallelism(3)
    data.print()
  }

  def SocketFunction(Env:StreamExecutionEnvironment): Unit ={
    val data = Env.socketTextStream("master",9999)
    data.print()
  }
}
