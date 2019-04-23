package com.tang.spark.flink.dataSet_j;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

public class readCsvFileApp {

    public static void main(String[] args) throws Exception {

        String input_path = "E:\\data\\flink\\people.csv";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        CsvReader csvReader = env.readCsvFile(input_path);
//        List<People> datas = csvReader.ignoreFirstLine().
//                fieldDelimiter(",").pojoType(People.class,"name","age","address").collect();

        List<Tuple3<String, Integer, String>> datas = csvReader.ignoreFirstLine().
                fieldDelimiter(",").includeFields("111").types(String.class, Integer.class, String.class).collect();
        System.out.println(datas);

    }
}
