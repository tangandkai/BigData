package com.tang.spark.sparkStreamingImooc

import com.tang.spark.dstream.StreamingExamples
import com.tang.spark.sparkStreamingImooc.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.tang.spark.sparkStreamingImooc.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.tang.spark.sparkStreamingImooc.utils.DatetUtils
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer

/**
  * 使用sparkStreaming处理kafka数据
  */
object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(20))

    val topics = Array("sparkStreaming").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.152.128:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
//      "heartbeat.interval.ms" -> 6000,
//      "session.timeout.ms" -> 6000,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list = new ListBuffer[ClickLog]
        partitionRecords.foreach(info=>{
          val splits = info.value().split("\t")
          var ip = " "
          var time = " "
          var courseId = 0
          var statusCode = 0
          var referer = " "
          if (splits.length == 5){
            ip = splits(0)
            time = DatetUtils.parse(splits(1))
            statusCode = splits(3).toInt
            referer = splits(4)
            //"GET /class/128.html HTTP/1.1"
            val url = splits(2).split(" ")(1) //    url = /class/128.html
            //获取实战课程编号
            if (url.startsWith("/class")){
              val courseHtmlId = url.split("/")(2)  // 128.html
              courseId = courseHtmlId.substring(0,courseHtmlId.lastIndexOf(".")).toInt
            }
          }
          list.append(ClickLog(ip,time,courseId,statusCode,referer))
        })
        print(list)
    })
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    //60.131.120.54	2019-04-08 14:46:01	"GET /class/145.html HTTP/1.1"	500	https://search.yahoo.com/search?p=Flume应用实战
//    val cleanDate = stream.map(record=>record.value()).map(line=>{
//      val infos = line.split("\t")
//      var ip = " "
//      var time = " "
//      var courseId = 0
//      var statusCode = 0
//      var referer = " "
//      if (infos.length == 5){
//        ip = infos(0)
//        time = DatetUtils.parse(infos(1))
//        statusCode = infos(3).toInt
//        referer = infos(4)
//        //"GET /class/128.html HTTP/1.1"
//        val url = infos(2).split(" ")(1) //    url = /class/128.html
//        //获取实战课程编号
//        if (url.startsWith("/class")){
//          val courseHtmlId = url.split("/")(2)  // 128.html
//          courseId = courseHtmlId.substring(0,courseHtmlId.lastIndexOf(".")).toInt
//        }
//      }
//
//      ClickLog(ip,time,courseId,statusCode,referer)
//    }).filter(clickLog=>clickLog.courseId != 0)
//
//    cleanDate.map(record=>(record.time.substring(0,8)+"_"+record.courseId,1)).
//      reduceByKey(_+_).foreachRDD(records=>{
//      records.foreachPartition(partitionRecords=>{
//        val list = new ListBuffer[CourseClickCount]
//        partitionRecords.foreach(info=>{
//          list.append(CourseClickCount(info._1,info._2))
//        })
//        CourseClickCountDAO.save(list)
//      })
//    })
//
//    cleanDate.map(record=>{
//      //https://search.yahoo.com/search?p=Flume应用实战
//      val referer = record.referer.replaceAll("//","/")
//      val splits = referer.split("/")
//      var host = ""
//      if (splits.length>1){
//        host = splits(1)
//      }
//      (host,record.courseId,record.time.substring(0,8))
//    }).filter(_._1 !="").map(record=>(record._3+"_"+record._1+"_"+record._2,1)).
//      reduceByKey(_+_).foreachRDD(records=>{
//      records.foreachPartition(partitionRecords=>{
//        val list = new ListBuffer[CourseSearchClickCount]
//        partitionRecords.foreach(info=>{
//          list.append(CourseSearchClickCount(info._1,info._2))
//        })
//        CourseSearchClickCountDAO.save(list)
//      })})
    ssc.start()
    ssc.awaitTermination()
  }
}
