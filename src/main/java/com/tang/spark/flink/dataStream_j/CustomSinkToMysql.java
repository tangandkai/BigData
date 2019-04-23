package com.tang.spark.flink.dataStream_j;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomSinkToMysql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sopurce = env.socketTextStream("master", 9999);
        SingleOutputStreamOperator<Student> data = sopurce.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] splits = value.split(",");
                Student student = new Student();
                if (splits != null && splits.length == 3) {
                    student.setId(Integer.parseInt(splits[0]));
                    student.setName(splits[1]);
                    student.setAge(Integer.parseInt(splits[2]));
                    return student;
                }
                return null;
            }
        });
        data.addSink(new MysqlSinkFunc());

        env.execute("CustomSinkToMysql");
    }
}
