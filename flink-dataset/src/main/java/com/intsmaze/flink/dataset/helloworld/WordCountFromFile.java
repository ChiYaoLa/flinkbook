package com.intsmaze.flink.dataset.helloworld;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Project: flink-book
 *
 * @author xuliang98
 * File Created at 2021/2/24-3:31 下午
 * @Desc
 */

public class WordCountFromFile {

    public static String filePath = "file:////Users/xuliang98/Documents/java/iot/flink-book/flink-dataset/src/main/resources/englishwords";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> text = env.readTextFile(filePath);


        // demo1: 简单的文本计算词频
        FlatMapOperator<String, Tuple2<String, Integer>> word = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.split("\\.");
                for (String token :
                        tokens) {
                    collector.collect(new Tuple2(token, 1));
                }
            }
        });
        AggregateOperator<Tuple2<String, Integer>> counts = word.groupBy(0).sum(1);

        counts.print();
//        env.execute("words");  //本地运行一般可以注释，因为flink往往需要数据流输入一直都存在，不然执行execute会报错
    }
}
