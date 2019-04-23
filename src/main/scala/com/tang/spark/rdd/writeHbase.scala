package com.tang.spark.rdd

//writeHbase
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object writeHbase {

  def main(args: Array[String]): Unit = {

    val input = "hdfs://master:9000/user/hadoop/input/student.txt"
    val hbconf = HBaseConfiguration.create()
    //hbase conf 设置
    hbconf.set("hbase.zookeeper.quorum","192.168.152.128:2181")
    hbconf.set("hbase.zookeeper.property.clientPort", "2181")

    val sparkConf = new SparkConf().setAppName("writeHbase").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
//    val spark = SparkSession.builder().appName("writeHbase").master("local[2]").getOrCreate()
//    val sc = spark.sparkContext

    val stuRDD = sc.textFile(input)

    val jobconf = new JobConf(hbconf)
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, "student")

    val rdd = stuRDD.map(_.split("\t")).map{arr=>{

      val put = new Put(Bytes.toBytes(arr(0).toInt))
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))  //插入名字
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))  //插入性别
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3).toInt)) //插入年龄
      (new ImmutableBytesWritable,put)
    }}

    rdd.saveAsHadoopDataset(jobconf)
    sc.stop()




//    val sess = SparkSession.builder().appName("wangjk").master("local[2]")
//      .config("spark.testing.memory", "2147480000").getOrCreate();
//    val sc = sess.sparkContext;
//
//    val tablename = "student"
//    val conf = HBaseConfiguration.create()
//    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
//    conf.set("hbase.zookeeper.quorum","master:2181")
//    //设置zookeeper连接端口，默认2181
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//
//    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
//    val jobConf = new JobConf(conf)
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
//
//    val indataRDD = sc.textFile(input)
//
//
//    val rdd = indataRDD.map(_.split("\t")).map{arr=>{
//      /*一个Put对象就是一行记录，在构造方法中指定主键
//       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
//       * Put.add方法接收三个参数：列族，列名，数据
//       */
//      val put = new Put(Bytes.toBytes(arr(0).toInt))
//      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))  //插入性别
//      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))  //插入性别
//      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3).toInt))  //插入性别
//      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
//      (new ImmutableBytesWritable, put)
//    }}
//
//    rdd.saveAsHadoopDataset(jobConf)
//
//    sc.stop()
//
//    sess.stop()
  }
}
