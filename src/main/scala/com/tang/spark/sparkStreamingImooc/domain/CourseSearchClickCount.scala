package com.tang.spark.sparkStreamingImooc.domain

/**
  * 实战课程从搜索引擎过来的点击数
  * @param day_search_course
  * @param click_count
  */
case class CourseSearchClickCount(day_search_course:String,click_count:Long)
