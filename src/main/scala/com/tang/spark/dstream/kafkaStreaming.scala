package com.tang.spark.dstream

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object kafkaStreaming {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("kafkaStreaming").setMaster("local[2]")
    StreamingExamples.setStreamingLogLevels()
    val ssc = new StreamingContext(conf,Seconds(5))

    val topics = Array("spark")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.152.128:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String,String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

//    stream.map(record => (record.key, record.value)).print()

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    stream.map(record=>record.value).flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()

  }

}
