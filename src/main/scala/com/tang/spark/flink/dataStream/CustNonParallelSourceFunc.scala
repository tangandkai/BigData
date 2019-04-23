package com.tang.spark.flink.dataStream

import org.apache.flink.streaming.api.functions.source.SourceFunction

class CustNonParallelSourceFunc extends SourceFunction[Long]{

  var count = 0L
  var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      ctx.collect(count)
      count+=1
      Thread.sleep(2000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
