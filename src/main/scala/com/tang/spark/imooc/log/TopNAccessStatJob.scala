package com.tang.spark.imooc.log

import com.tang.spark.imooc.caseclass.{DayCityVideoAccessStat, DayTrafficAccessStat, DayVideoAccessStat}
import com.tang.spark.imooc.dao.StatDAO
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object TopNAccessStatJob {

  def main(args: Array[String]): Unit = {

    val input = "hdfs://master:9000/user/hadoop/imooc_clean"
    val spark = SparkSession.builder().appName("TopNStatJob").
      config("spark.sql.sources.partitionColumnTypeInference.enabled",false).
      master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load(input)

    import spark.implicits._
    val commomDF = accessDF.filter($"cmsType"==="video")
    commomDF.persist()

    //获取各天课程访问topn的数据
//    val videoTopN = videoAccessTopNStat(spark,commomDF)

    //将获取到的每天课程访问topn数据插入到mysql
    //    isnertVideoTopnToMysql(videoTopN)


    //获取每天各城市课程访问前topn的数据
//   videoCityAccessTopNStat(spark,commomDF)

    //根据流量统计访问前topn的课程
    videoTrafficAccessTopNStat(spark,commomDF)

    commomDF.unpersist()
    spark.stop()
  }


  /**
    * 统计排名为topn的课程
    * @param spark
    * @param accessDF
    * @return
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame) ={

    /**
      * 使用df的方式进行统计
      */
    import spark.implicits._
    val videoTopN = accessDF.groupBy($"day",$"cmsId").
      agg(count("*") as("times")).orderBy($"times".desc)
    videoTopN
  }


  /**
    *  将获取到的每天课程访问topn数据插入到mysql
    * @param data
    */
  def isnertVideoTopnToMysql(data: Dataset[Row]): Unit ={


    try{
      data.foreachPartition(partitionRecords=>{
        val list = new ListBuffer[DayVideoAccessStat]
        partitionRecords.foreach(info =>{
          val day = info.getAs[String]("day").trim
          val cms_id = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          list.append(DayVideoAccessStat(day,cms_id,times))
        })
        StatDAO.insertDayVideoAccessTopN(list)
      })
    }
    catch {
      case e:Exception => e.printStackTrace()
    }
  }


  /**
    * 按照城市统计排名为topN的课程
    * @param spark
    * @param accessDF
    * @return
    */
  def videoCityAccessTopNStat(spark: SparkSession, accessDF: DataFrame){

    import spark.implicits._
    val cityAccessTopNDF = accessDF.select($"day",$"city",$"cmsId").
      groupBy($"day",$"cmsId",$"city").agg(count("*")alias("times")).orderBy($"times".desc)

    //使用窗口统计每个城市排名前三的访问课程
    val videoCityDF =  cityAccessTopNDF.select($"day",$"cmsId",$"city",$"times",
      row_number().over(Window.partitionBy($"city").orderBy($"times".desc))alias("ranks")
    ).filter($"ranks"<=3)
    try{
      videoCityDF.foreachPartition(partitionRecords=>{
        val list = new ListBuffer[DayCityVideoAccessStat]
        partitionRecords.foreach(info =>{
          val day = info.getAs[String]("day").trim
          val cms_id = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city").trim
          val times = info.getAs[Long]("times")
          val ranks = info.getAs[Int]("ranks")
          list.append(DayCityVideoAccessStat(day,cms_id,city,times,ranks))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    }catch {
      case e:Exception => {
        println("+++++++++++++++++++++++")
        e.printStackTrace()
      }
    }

  }

  /**
    * 按照流量统计访问topn的课程
    * @param spark
    * @param accessDF
    */
  def videoTrafficAccessTopNStat(spark: SparkSession,accessDF:DataFrame) = {

    import spark.implicits._
    val videoTrafficDF = accessDF.groupBy($"day", $"cmsId").
      agg(sum($"traffic") alias ("traffics")).orderBy($"traffics".desc)
    videoTrafficDF.show(false)

    try {
      videoTrafficDF.foreachPartition(partitionRecords => {
        val list = new ListBuffer[DayTrafficAccessStat]
        partitionRecords.foreach(info => {
          val day = info.getAs[String]("day").trim
          val cms_id = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          list.append(DayTrafficAccessStat(
            day, cms_id, traffics))
        })
        StatDAO.insertDayTrafficsVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => {
        println("+++++++++++++++++++++++")
        e.printStackTrace()
      }
    }
  }
}
