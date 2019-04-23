package com.tang.spark.imooc.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换工具类
  */
object AccessConvertUtils {

  val struct = StructType(
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),
      StructField("cmsId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  )

  /**
    * 日志转换，将rdd转成df
    * @param log
    */
  def parseLog(log:String) ={

    try{
      val splits = log.split("\t")
      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
//      println(url.indexOf(domain))
      val cmsTypeId = cms.split("/")
//      println(cms)
//      cmsTypeId.foreach(println)
      var cmsType = ""
      var cmsId = 0l
      if(cmsTypeId.length > 1){
          cmsType = cmsTypeId(0)
          cmsId = cmsTypeId(1).toLong
        }
      val city = IPUtils.getCity(ip)
      val time = splits(0)
      val day = time.substring(0,10).replaceAll("-","")

      //Row中的字段类型要与上面定义的struct中的字段类型对应上
      Row(url,cmsType,cmsId,traffic,ip,city,time,day)
    } catch {
      case e:Exception => Row(0)
    }

  }

  def main(args: Array[String]): Unit = {

    val row = parseLog("2016-11-10 00:01:12\thttp://www.imooc.com/wenda/detail/320676.html\t7573\t220.181.108.95")
    println(row.toString())
  }
}
