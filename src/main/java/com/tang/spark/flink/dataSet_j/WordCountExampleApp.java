package com.tang.spark.flink.dataSet_j;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountExampleApp {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.fromElements("Who's there?",
                "I think I hear them. Stand, ho! Who's there?");
        text.print();

//        text.output(Jd)
//        text.flatMap(new FlatMapFunction<String, WC>() {
//            @Override
//            public void flatMap(String value, Collector<WC> out) throws Exception {
//                String[] tokens = value.toLowerCase().trim().split(" ");
//                for (String token:tokens){
//                    out.collect(new WC(token,1));
//                }
//            }
//        }).groupBy("word").sum(1).print();

        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.toLowerCase().trim().split(" ");
                for (String token:tokens){
                    out.collect(new Tuple2(token,1));
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
