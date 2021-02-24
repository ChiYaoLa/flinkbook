package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.hamcrest.internal.ArrayIterator;

import java.util.ArrayList;

/**
 * github地址: https://github.com/ChiYaoLa
 *
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class FilterTemplate {


    /**
     * github地址: https://github.com/ChiYaoLa
     *
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {


        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //过滤序列
        DataSet<Long> dataSet = env.generateSequence(1, 5);

        DataSet<Long> filterDataSet = dataSet.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) {
                if (value == 2L || value == 4L) {
                    return false;
                }
                return true;
            }
        });

        filterDataSet.print("输出结果");
//        env.execute("Filter Template");


        // 过滤单词文本
        String filePath = "file:////Users/xuliang98/Documents/java/iot/flink-book/flink-dataset/src/main/resources/englishwords";

        env.readTextFile(filePath).flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] tokens = s.split("\\.");
                for (String token :
                        tokens) {
                    collector.collect(token);
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                if (s.startsWith("h")){
                    return true;
                }
                return false;
            }
        }).print("打印出所有是h开头的单词");

        env.execute();
    }
}