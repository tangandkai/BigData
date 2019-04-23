package com.tang.spark.imooc.caseclass

/**
  * 每天课程访问次数实体类
  * @param day
  * @param cms_id
  * @param times
  */
case class DayVideoAccessStat(day:String,cms_id:Long,times:Long)
