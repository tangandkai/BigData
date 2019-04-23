package com.tang.spark.sparkStreamingImooc.dao

import com.tang.spark.sparkStreamingImooc.domain.CourseClickCount
import com.tang.spark.utils.HbaseUtils
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 课程点击量数据访问层
  */
object CourseClickCountDAO {

  val tableName = "imooc_course_clickcount"
  val cf = "info"

  val qualifer = "click_count"

  /**
    * 保存数据到hbase
    * @param list
    */
  def save(list:ListBuffer[CourseClickCount]): Unit ={

    val table = HbaseUtils.getInstance().getTable(tableName)
    for (ele <- list){
      table.incrementColumnValue(
        Bytes.toBytes(ele.day_course),
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

    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20181123_9",8))
    list.append(CourseClickCount("20181124_99",81))
    list.append(CourseClickCount("20181125_29",18))
    list.append(CourseClickCount("20181126_19",28))
    list.append(CourseClickCount("20181127_91",84))
    save(list)
    println(Bytes.toLong(get("20181124_99")))
  }
}
