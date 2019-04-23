package com.tang.spark.df

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class Person(id:Int,name:String,gender:String,age:Int)
object rddToDf {

  def main(args: Array[String]): Unit = {

    //first
    val path = "hdfs://master:9000/user/hadoop/input/student.txt"

    val spark = SparkSession.builder().master("local[2]").appName("rddToDf").getOrCreate()
    val rdd = spark.sparkContext.textFile(path)
    rdd.foreach(println)

    import spark.implicits._
    val peopleDF = rdd.map(_.split("\t")).map(attributes=>
      Person(attributes(0).toInt,attributes(1),attributes(2),attributes(3).toInt)).toDF()

    peopleDF.show()
    peopleDF.createTempView("peo")
    spark.sql("select name,gender,age from peo where age>10 ").show()

    peopleDF.select("name","gender","age").filter(peopleDF("age")>10).groupBy("name","gender","age").count().show()


    //second
    val path_1 = "hdfs://master:9000/user/hadoop/wordcount.txt"
    val rdd_1 = spark.sparkContext.textFile(path)
    rdd_1.foreach(println)

    val schemaString = "id,name,gender,age"
    val fields = schemaString.split(",").map(fieldName=>StructField(fieldName,StringType,true))
    val schema = StructType(fields)
    val rowRDD = rdd_1.map(_.split("\t")).map(args=>Row(args(0),args(1),args(2),args(3)))
    val peoDF = spark.createDataFrame(rowRDD,schema)
    peoDF.map(x=>x+"-").show()
    val ds = peoDF.as[Person]
    ds.printSchema()
    ds.map(x=>x.name).show()
    peoDF.show()

    spark.stop()
  }

}
