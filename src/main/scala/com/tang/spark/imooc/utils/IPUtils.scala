package com.tang.spark.imooc.utils

import com.ggstar.util.ip.IpHelper

/**
  * ip解析工具类
  */
object IPUtils {


  def getCity(ip:String) ={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {

    val ip = "58.30.15.255"
    val region = getCity(ip)
    System.out.println(region)
  }

}
