package com.tang.spark.flink.dataStream_j;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlSinkFunc extends RichSinkFunction<Student> {

    private int count = 0;
    private Connection conn = null;
    private PreparedStatement pstmt = null;
    private final String url = "jdbc:mysql://master:3306/flink?user=root&password=123456&useSSL=false";

    /**
     * 获取数据库连接
     * @return
     */
    private void getConnection(){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭数据库连接
     */
    private void release(){
        if (pstmt!=null){
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("start init.....");
        getConnection();
        String sql = "insert into student(Id,name,age) values(?,?,?) ON DUPLICATE KEY UPDATE name = ?,age = ?";
        pstmt = conn.prepareStatement(sql);
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
        count++;
        System.out.println("The "+count+" time start insert data into student.....");
        pstmt.setInt(1,value.getId());
        pstmt.setString(2,value.getName());
        pstmt.setInt(3,value.getAge());
        pstmt.setString(4,value.getName());
        pstmt.setInt(5,value.getAge());
        pstmt.executeUpdate();
    }

    @Override
    public void close() {
        System.out.println("close the connection of the mysql.....");
        release();
    }
}
