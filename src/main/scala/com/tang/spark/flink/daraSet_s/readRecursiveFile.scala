package com.tang.spark.flink.daraSet_s

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object readRecursiveFile {

  def main(args: Array[String]): Unit = {

  val input_path = "E:\\data\\flink"
    val env = ExecutionEnvironment.getExecutionEnvironment

    val conf = new Configuration();
    conf.setBoolean("recursive.file.enumeration",true)
    val text = env.readTextFile(input_path).withParameters(conf)
    text.print()
  }
}
