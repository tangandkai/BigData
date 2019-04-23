package com.tang.spark.sparkStreamingImooc.dao

import com.tang.spark.sparkStreamingImooc.domain.{CourseClickCount, CourseSearchClickCount}
import com.tang.spark.utils.HbaseUtils
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CourseSearchClickCountDAO {


  val tableName = "imooc_course_search_clickcount"
  val cf = "info"

  val qualifer = "click_search_count"

  /**
    * 保存数据到hbase
    * @param list
    */
  def save(list:ListBuffer[CourseSearchClickCount]): Unit ={

    val table = HbaseUtils.getInstance().getTable(tableName)
    for (ele <- list){
      table.incrementColumnValue(
        Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
    HbaseUtils.getInstance().release()
  }

  /**
    * 根据rowkey查询数据
    * @param day_course
    * @return
    */
  def get(day_course:String)={
    val value = HbaseUtils.getInstance().get(tableName,day_course,cf,qualifer)
    HbaseUtils.getInstance().release()
    value
  }

  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("20181123_www.baidu.com_9",8))
    list.append(CourseSearchClickCount("20181124_www.sogou.com_99",81))
    list.append(CourseSearchClickCount("20181125_search.yahoo.com_29",18))
    list.append(CourseSearchClickCount("20181126_cn.bing.com_19",28))
    save(list)
    println(Bytes.toLong(get("20181123_www.baidu.com_9")))
  }
}
