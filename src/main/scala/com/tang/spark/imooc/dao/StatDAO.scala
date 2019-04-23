package com.tang.spark.imooc.dao

import java.sql.{Connection, PreparedStatement}

import com.tang.spark.imooc.caseclass.{DayCityVideoAccessStat, DayTrafficAccessStat, DayVideoAccessStat}
import com.tang.spark.imooc.utils.MysqlUtils

import scala.collection.mutable.ListBuffer

/**
  * 统计各个维度
  */
object StatDAO {


  /**
    * 统计每天课程访问前topn的课程
    * @param list
    */
  def insertDayVideoAccessTopN(list:ListBuffer[DayVideoAccessStat]): Unit ={

    var conn:Connection = null
    var pstmt:PreparedStatement = null

    try{
      val sql = "insert into day_videa_topn_stat(day,cms_id,times) values (?,?,?)"
      conn = MysqlUtils.getConnection()
      conn.setAutoCommit(false)
      pstmt = conn.prepareStatement(sql)
      for (ele <- list){
//        println(ele.toString)
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cms_id)
        pstmt.setLong(3,ele.times)
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


  /**
    * 统计每天各城市访问课程前topn
    * @param list
    */
  def insertDayCityVideoAccessTopN(list:ListBuffer[DayCityVideoAccessStat]): Unit ={

    var conn:Connection = null
    var pstmt:PreparedStatement = null

    try{
      val sql = "insert into day_video_city_topn_stat(day,cms_id,city,times,ranks) values (?,?,?,?,?)"
      conn = MysqlUtils.getConnection()
      conn.setAutoCommit(false)
      pstmt = conn.prepareStatement(sql)
      for (ele <- list){
        println(ele.toString)
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cms_id)
        pstmt.setString(3,ele.city)
        pstmt.setLong(4,ele.times)
        pstmt.setInt(5,ele.ranks)
        println(ele.day+":"+ele.cms_id+":"+ele.city+":"+ele.times+":"+ele.ranks)
        pstmt.addBatch()
      }
      pstmt.executeBatch()  //执行批量处理
      conn.commit()
    }catch {
      case e:Exception => {
        println("--------------------------------")
        e.printStackTrace()
        conn.rollback()
      }
    }
    finally {
      MysqlUtils.release(conn,pstmt)
    }
  }

  /**
    * 按照流量统计访问前topn的课程，将结果插入mysql
    */
  def insertDayTrafficsVideoAccessTopN(list:ListBuffer[DayTrafficAccessStat]): Unit ={

    var conn:Connection = null
    var pstmt:PreparedStatement = null

    try{
      val sql = "insert into day_video_traffics_topn_stat(day,cms_id,traffics) values (?,?,?)"
      conn = MysqlUtils.getConnection()
      conn.setAutoCommit(false)
      pstmt = conn.prepareStatement(sql)
      for (ele <- list){
//        println(ele.toString)
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cms_id)
        pstmt.setLong(3,ele.traffics)
        pstmt.addBatch()
      }
      pstmt.executeBatch()  //执行批量处理
      conn.commit()
    }catch {
      case e:Exception => {
        println("--------------------------------")
        e.printStackTrace()
        conn.rollback()
      }
    }
    finally {
      MysqlUtils.release(conn,pstmt)
    }
  }

}
