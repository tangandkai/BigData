package com.tang.spark.flink;

import com.tang.spark.flink.dataSet_j.WC;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingAnalysis {

    public static void main(String[] args) throws Exception {

        String host = "master";
        Integer port = 9999;
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = see.socketTextStream(host, port);
//        source.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] tokens = value.split(" ");
//                for (String token:tokens){
//                    collector.collect(new Tuple2<String, Integer>(token,1));
//                }
//            }
//        }).keyBy(0).timeWindow(Time.seconds(10)).sum(1).print().setParallelism(1);

//        source.flatMap(new FlatMapFunction<String, WC>() {
//            @Override
//            public void flatMap(String value, Collector<WC> out) throws Exception {
//                String[] tokens = value.split(" ");
//                for (String token:tokens){
//                    out.collect(new WC(token,1));
//                }
//            }
//        }).keyBy("word").timeWindow(Time.seconds(10)).sum("count").print().setParallelism(1);


        source.flatMap(new FlatMapFunction<String, WC<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<WC<String,Integer>> out) throws Exception {
                String[] tokens = value.split(" ");
                for (String token:tokens){
                    out.collect(new WC(token,1));
                }
            }
        }).keyBy(new KeySelector<WC<String,Integer>, String>() {
            @Override
            public String getKey(WC<String,Integer> value) throws Exception {
                return value.getWord();
            }
        }).timeWindow(Time.seconds(10)).sum("count").print().setParallelism(1);
        see.execute("StreamingAnalysis");
    }
}
