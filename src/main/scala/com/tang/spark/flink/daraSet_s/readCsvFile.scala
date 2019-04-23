package com.tang.spark.flink.daraSet_s

import com.tang.spark.flink.dataSet_j.People
import org.apache.flink.api.scala.ExecutionEnvironment

object readCsvFile {

//  case class People_1(name:String,age:Int,address:String)
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val input_path = "E:\\data\\flink\\people.csv"
    import org.apache.flink.api.scala._
//    env.readCsvFileApp[(String,Int,String)](filePath=input_path,
//      ignoreFirstLine=true
//    ).print()
//    env.readCsvFileApp[(String,Int)](filePath=input_path,
//      ignoreFirstLine=true,includedFields = Array(0,1)
//    ).print()

//    env.readCsvFileApp[(String,String)](filePath=input_path,
//      ignoreFirstLine=true,includedFields = Array(0,2)
//    ).print()

//    env.readCsvFile[People_1](filePath=input_path,
//      ignoreFirstLine=true
//    ).print()

    env.readCsvFile[People](filePath=input_path,
      ignoreFirstLine=true,pojoFields = Array("name","age","address")
    ).print()
  }
}
