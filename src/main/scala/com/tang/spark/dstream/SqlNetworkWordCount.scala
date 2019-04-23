package com.tang.spark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SqlNetworkWordCount {

  def main(args: Array[String]): Unit = {

    StreamingExamples.setStreamingLogLevels()
    val conf = new SparkConf()
    conf.setAppName("SqlNetworkWordCount")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val lines = ssc.socketTextStream("192.168.152.128",9999)
    val words = lines.flatMap(_.split(" "))
    words.foreachRDD(rdd=>{
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      wordsDataFrame.select($"word").groupBy($"word").agg(count("*")alias("total")).show()
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
//      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }
  )
    ssc.start()
    ssc.awaitTermination()
  }
}

  /** Case class for converting RDD to DataFrame */
  case class Record(word: String)

  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {
    @transient  private var instance: SparkSession = _
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
}
