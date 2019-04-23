package com.tang.spark.imooc.log

import com.tang.spark.imooc.utils.{AccessConvertUtils, SparkStateFormatUtils}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * 使用spark完成数据清洗操作
  */
object SparkStateCleanJob {

  def main(args: Array[String]): Unit = {

    val input = "hdfs://master:9000/user/hadoop/input/access.20161111.log"
    val output = "hdfs://master:9000/user/hadoop/imooc_clean"

    val spark = SparkSession.builder().appName("SparkStateCleanJob").
      master("local[2]").getOrCreate()

    val formatRDD = SparkStateFormatUtils.parse(spark,input)
//
    formatRDD.take(20).foreach(println)

    //将formatRDD转成df
    val formatDF = spark.createDataFrame(
      formatRDD.map(line=>AccessConvertUtils.parseLog(line)).filter(x=>x.equals(Row(0)).unary_!),AccessConvertUtils.struct)

    formatDF.printSchema()
//    formatDF.show(false)

//    formatDF.write.format("parquet").
//      mode("overwrite").partitionBy("day").saveAsTable("imooc_clean_log")
    formatDF.coalesce(1).write.format("parquet").
      mode("overwrite").partitionBy("day").save(output)


    spark.stop()
  }
}
