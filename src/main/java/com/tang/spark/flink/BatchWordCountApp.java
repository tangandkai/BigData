package com.tang.spark.flink;

import com.tang.spark.flink.dataSet_j.WC;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCountApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> file = env.readTextFile("hdfs://master:9000/user/hadoop/wordcount.txt");
        file.print();
        AggregateOperator<Tuple2<String, Integer>> wordCount = file.flatMap(new Tokenizer()).groupBy(0)
                .sum(1);
        System.out.println(wordCount.first(2));

        file.flatMap(new Tokenizer_1()).groupBy("word").sum(1).print();
        wordCount.print();
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\n");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

    public static class Tokenizer_1 implements FlatMapFunction<String, WC<String,Integer>> {
        @Override
        public void flatMap(String value, Collector<WC<String,Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\n");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new WC(token, 1));
                }
            }
        }
    }
}

