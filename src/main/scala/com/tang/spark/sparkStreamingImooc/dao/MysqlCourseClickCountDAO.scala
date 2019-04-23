package com.tang.spark.sparkStreamingImooc.dao

import java.sql.{Connection, PreparedStatement}

import com.tang.spark.imooc.utils.MysqlUtils
import com.tang.spark.sparkStreamingImooc.domain.CourseClickCount

import scala.collection.mutable.ListBuffer

object MysqlCourseClickCountDAO {


  /**
    * 统计实战课程点击量
    * @param list
    */
  def insertCourseClickCount(list:ListBuffer[CourseClickCount]): Unit ={

    var conn:Connection = null
    var pstmt:PreparedStatement = null

    try{
      val sql = "insert into imooc_course_clickcount(day_course,click_count) values (?,?) ON DUPLICATE KEY UPDATE click_count=? + click_count"
      conn = MysqlUtils.getConnection()
      conn.setAutoCommit(false)
      pstmt = conn.prepareStatement(sql)
      for (ele <- list){
        pstmt.setString(1,ele.day_course)
        pstmt.setLong(2,ele.click_count)
        pstmt.setLong(3,ele.click_count)
        pstmt.addBatch()
      }
      pstmt.executeBatch()  //执行批量处理
      conn.commit()
    }catch {
      case e:Exception => e.printStackTrace()
    }
    finally {
      MysqlUtils.release(conn,pstmt)
    }
  }


}
