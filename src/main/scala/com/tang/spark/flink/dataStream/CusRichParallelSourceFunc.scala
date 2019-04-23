package com.tang.spark.flink.dataStream

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class CusRichParallelSourceFunc extends RichParallelSourceFunction[Long]{


  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {

  }

  override def cancel(): Unit = {

  }
}
