package com.tang.spark.rdd

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object readHbase {

  def main(args: Array[String]): Unit = {

    val configuration = new SparkConf().setAppName("readHbase").setMaster("local[2]")
    val sc = new SparkContext(configuration)

    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum", "192.168.152.128:2181");
    //设置查询的表名
    hbaseConfiguration.set(TableInputFormat.INPUT_TABLE,"student")

    //读取hbase配置
    val stuRDD = sc.newAPIHadoopRDD(hbaseConfiguration,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
//    new HBaseContext()

    val count = stuRDD.count()
    println("the stuRDD count: "+count)

    stuRDD.foreach({case(_,result)=>

        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("info".getBytes(),"name".getBytes()))
        val gender = Bytes.toString(result.getValue("info".getBytes(),"gender".getBytes()))
        val age = Bytes.toString(result.getValue("info".getBytes(),"age".getBytes()))
        println("key="+key+"\n"+
        "name="+name+"\n"+
        "gender="+gender+"\n"+
        "age="+age)
    })

    sc.stop()
  }

}
