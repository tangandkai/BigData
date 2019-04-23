package com.tang.spark.sparkStreamingImooc.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DatetUtils {

  /**
    * 输入的时间格式
    * SimpleDateFormat 线程不安全
    * FastDateFormat   线程安全
    */
  val inputFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss",Locale.ENGLISH)
  /**
    * 要转化的成的时间格式
    */
  val outputFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")
  def parse(time:String) ={

    outputFormat.format(new Date(getTime(time)))
  }

  /**
    * @param time
    * @return
    */
  def getTime(time:String) ={
    try{
      inputFormat.parse(time).getTime
    }
    catch {
      case e:Exception=>{
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("2019-04-07 17:12:01"))
  }
}
