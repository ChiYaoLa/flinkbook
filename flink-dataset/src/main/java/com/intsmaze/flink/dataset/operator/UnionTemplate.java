package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * github地址: https://github.com/ChiYaoLa
 *
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class UnionTemplate {

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

        DataSet<Long> dataSetOne = env.generateSequence(1, 2);

        DataSet<Long> dataSetTwo = env.generateSequence(1001, 1002);

        DataSet<Long> union = dataSetOne.union(dataSetTwo);
        union.print("输出结果");
        env.execute("Union Template");
    }
}
