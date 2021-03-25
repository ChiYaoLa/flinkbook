package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

import static org.apache.flink.api.java.aggregation.Aggregations.MIN;
import static org.apache.flink.api.java.aggregation.Aggregations.SUM;


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
public class AggregationsTemplate {


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
        List<Tuple3<String, Integer, Double>> list = new ArrayList<>();
        list.add(new Tuple3<>("张三", 15, 999.9));
        list.add(new Tuple3<>("张三", 30, 1899.0));
        list.add(new Tuple3<>("张三", 21, 3000.89));
        list.add(new Tuple3<>("李四", 31, 188.88));
        list.add(new Tuple3<>("王五", 55, 99.99));
        list.add(new Tuple3<>("王五", 67, 18.88));

        DataSource<Tuple3<String, Integer, Double>> dataSource = env.fromCollection(list);

//        dataSource.groupBy("f0").sum(0)
//                .aggregate(SUM, 1)
//                .print("aggregate sum");

//        dataSource.groupBy("f0")
//                .aggregate(SUM, 1)
//                .and(MIN, 2)
//                .print("aggregate sum and min");

//        dataSource.groupBy(0).aggregate(SUM,1).print("aggre sum");
//        dataSource.groupBy(0).min(1).print("groupby min");

//        dataSource.groupBy("f0")
//                .sum(1).print("sum");
//
//        dataSource.groupBy("f0")
//                .max(1)
//                .print("max");
//
//
//        dataSource
//                .groupBy("f0")
//                .minBy(1)
//                .print("minBy");
//
//        dataSource
//                .groupBy("f0")
//                .maxBy(1)
//                .print("maxBy");


        dataSource.groupBy(0).reduce(new ReduceFunction<Tuple3<String, Integer, Double>>() {
            @Override
            public Tuple3<String, Integer, Double> reduce(Tuple3<String, Integer, Double> t0, Tuple3<String, Integer, Double> t1) throws Exception {
                Tuple3<String, Integer, Double> res = new Tuple3<>();
                res.f1 = t0.f1+t1.f1*2;
                res.f0 = t0.f0+t1.f0; // 字符串也能相加
                res.f2 = 2*t0.f2+t1.f2;
                return res;
            }
        }).print("故意复杂");
        env.execute();
    }
}