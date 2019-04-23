package com.tang.spark.imooc.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateFormatUtils {

  /**
    * 输入的时间格式
    * SimpleDateFormat 线程不安全
    * FastDateFormat   线程安全
    */
//  val inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
  val inputFormat = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
  /**
    * 要转化的成的时间格式
    */
//  val outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val outputFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  def parse(time:String) ={

    outputFormat.format(new Date(getTime(time)))
  }

  /**
    * [10/Nov/2016:00:01:02 +0800]
    * 将传入的时间格式转成long
    * @param time
    * @return
    */
  def getTime(time:String) ={
    try{
      inputFormat.parse(time.substring(time.indexOf("[")+1,time.indexOf("]"))).getTime
    }
    catch {
      case e:Exception=>{
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02 +0800]"))
  }
}
