package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * github地址: https://github.com/ChiYaoLa
 *
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class DistinctTemplate {


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

        // 基本元素去重
        List<Tuple3<String, Integer, String>> list = new ArrayList<Tuple3<String, Integer, String>>();
        list.add(new Tuple3("185XXX", 899, "周一"));
        list.add(new Tuple3("155XXX", 1199, "周二"));
        list.add(new Tuple3("155XXX", 899, "周一"));

        DataSet<Tuple3<String, Integer, String>> dataSet = env.fromCollection(list);

        DataSet<Tuple3<String, Integer, String>> distinct = dataSet.distinct("f1", "f2");
        distinct.print("输出结果");
        env.execute("Distinct Template");


        // 文本去重demo
        String filePath = "file:////Users/xuliang98/Documents/java/iot/flink-book/flink-dataset/src/main/resources/englishwords";
        env.readTextFile(filePath).flatMap(new FlatMapFunction<String, Tuple1<String>>() {  // 故意用Tuple1
            @Override
            public void flatMap(String s, Collector<Tuple1<String>> collector) throws Exception {
                String[] tokens = s.split("\\.");
                for (String token :
                        tokens) {
                    collector.collect(new Tuple1<>(token));
                }
            }
        }).distinct(0).print();
//        env.execute("文本去重 demo");

    }
}
