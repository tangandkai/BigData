package com.tang.spark.df

import org.apache.spark.sql.SparkSession


object readjson {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("readjson").
      master("local[2]").getOrCreate()
    import spark.implicits._
    val path = "hdfs://master:9000/user/hadoop/input/people.json"
    //读取json文件，生成dataframe
    val df = spark.read.json(path)
    df.printSchema()

    df.select("name","age").show()
    df.select(df("age")>20).show()
    df.filter(df("age")>20).show()
    df.groupBy("age").count().show()
    df.sort(df("age").desc).show()

    df.createTempView("people")
    spark.sql("select * from people").show()
    spark.sql("select name,age from people where age is not null").show()

    spark.stop()
  }

}
