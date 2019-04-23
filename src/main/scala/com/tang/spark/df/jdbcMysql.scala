package com.tang.spark.df

import java.util.Properties

import org.apache.spark.sql.SparkSession

case class People(name:String,gender:String,age:Int,phone_num:String)
object jdbcMysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("jdbcMysql").master("local[2]").getOrCreate()

    //从mysql表中读取数据
    val jdbcDF = spark.read.format("jdbc").
      option("url","jdbc:mysql://master:3306/spark").
      option("driver","com.mysql.jdbc.Driver").
      option("dbtable","people").
      option("user","root").
      option("password","123456").load()

    jdbcDF.show()


//    val connectionProperties = new Properties()
//    connectionProperties.put("user","root")
//    connectionProperties.put("password","123456")
//
//    val jdbcDF_1 = spark.read.jdbc("jdbc:mysql://master:3306/study","people",connectionProperties)
//    jdbcDF_1.printSchema()
//    jdbcDF_1.show()

    //插入数据到mysql
//    val input = "hdfs://master:9000/user/hadoop/input/people.txt"
//
//    import spark.implicits._
//    val peopleDF = spark.sparkContext.textFile(input).map(_.split(",")).map(
//      attributions=>People(attributions(0),attributions(1),attributions(2).toInt,attributions(3))).toDF()
//
//
//    peopleDF.printSchema()
//    println("printSchema")
//    peopleDF.show()
//
//    peopleDF.write.mode("Append").jdbc("jdbc:mysql://master:3306/spark","spark.people",connectionProperties)

    spark.stop()
  }


}
