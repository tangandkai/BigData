package com.tang.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object helloword {

  def main(args: Array[String]): Unit = {

    val inputfile = "hdfs://192.168.152.128:9000/user/hadoop/wordcount.txt"
    //    val inputfile = "root@192.168.152.128:/data/rdd/*.txt"
    //    val inputfile = "E:\\data\\rdd\\wordcount.txt"
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textfile = sc.textFile(inputfile)
    val wordcount = textfile.flatMap(line=>line.split("\n")).map(word=>(word,1)).reduceByKey((a,b)=>a+b)
    wordcount.foreach(println)

    val books = sc.parallelize(Array(("hadoop",2),("hbase",4),("spark",5),("flume",1),("haoddp",1)))
    books.mapValues(x=>(x,1)).reduceByKey((a,b)=>(a._1+b._1,a._2+b._2)).mapValues(x=>x._1/x._2).foreach(println)

    val broadcast = sc.broadcast(Array(1,23,45,6))
    println("broadcast: "+broadcast)
    broadcast.value.foreach(x=>println("广播变量的值为："+x))

    val accumulator = sc.doubleAccumulator("测试累加器")
    sc.parallelize(Array(2,3,4,5)).foreach(x=>accumulator.add(x))
    println("累加器累加结果："+accumulator)
    //      textfile.saveAsTextFile("hdfs://192.168.74.128:9000/user/hadoop/word.txt")
    sc.stop()
  }

}
