package com.tang.spark.df

import org.apache.spark.sql.SparkSession

object readHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("readHive").master("local[2]").
      enableHiveSupport().getOrCreate()

    val hiveDF = spark.sql("select * from tang.user_info")
    hiveDF.printSchema()
    hiveDF.show()


    spark.stop()
  }

}
