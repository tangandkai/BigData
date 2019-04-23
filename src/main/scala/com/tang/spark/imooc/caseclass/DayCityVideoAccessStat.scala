package com.tang.spark.imooc.caseclass

/**
  * 每天各城市课程访问次数实体类
  * @param day
  * @param cms_id
  * @param city
  * @param times
  * @param ranks
  */
case class DayCityVideoAccessStat(day:String,cms_id:Long,city:String,times:Long,ranks:Int)

