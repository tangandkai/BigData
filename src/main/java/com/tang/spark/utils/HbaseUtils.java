package com.tang.spark.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HbaseUtils {

    private Connection conn = null;
    private Configuration configuration = null;
    private static HbaseUtils hbaseUtils = null;
    private HbaseUtils() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.152.128:2181");
        configuration.set("habse.rootdir","hdfs://192.168.152.128:9000/hbase");
        conn = ConnectionFactory.createConnection(configuration);
    }

    /**
     * 获取HbaseUtils实例
     * @return
     */
    public static synchronized HbaseUtils getInstance() throws IOException {
        if (null == hbaseUtils){
            return new HbaseUtils();
        }
        return hbaseUtils;
    }

    /**
     *
     * @param tableName
     * @return
     */
    public Table getTable(String tableName){
        Table hTable = null;
        try {
            hTable = conn.getTable(TableName.valueOf(tableName));
        }catch (Exception e){
            e.printStackTrace();
        }
        return hTable;
    }

    /**
     * 网hbase表中插入数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param colum
     * @param value
     */
    public void put(String tableName,String rowKey,String cf,String colum,String value) throws IOException {

        Table table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(colum),Bytes.toBytes(value));
        table.put(put);
    }

    /**
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param colum
     */
    public byte[] get(String tableName,String rowKey,String cf,String colum){
        Table table = getTable(tableName);
        Result result = null;
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(colum));
        try {
            result = table.get(get);
            byte[] value = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(colum));
//            System.out.println(value);
            return value;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void scan(){

    }
    /**
     * 释放hbase连接
     */
    public void release(){
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        String tableName = "imooc_course_clickcount";
        String rowKey = "20190408_134";
        String cf = "info";
        String colum = "click_count";
        String value = "34";
        HbaseUtils instance = HbaseUtils.getInstance();
        System.out.println(instance.getTable(tableName).getName().getNameAsString());
        System.out.println(instance.conn.getAdmin());
        instance.get(tableName,rowKey,cf,colum);

//        instance.put(tableName,rowKey,cf,colum,value);


        instance.release();
    }
}
