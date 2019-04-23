package com.tang.spark.test

object helloword {

  def main(args: Array[String]): Unit = {

    println("hello,nice to meet you...")
    val str = "1  2 3 4"
    val res = str.split("\t")
    println(res)
  }
}
