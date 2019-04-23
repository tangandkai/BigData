package com.tang.spark.sparkStreamingImooc.domain

/**
  * 实战课程点击量
  * @param day_course hbase中的rowkey
  * @param click_count 点击数
  */
case class CourseClickCount(day_course:String,click_count:Long)
