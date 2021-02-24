package com.intsmaze.flink.dataset.param;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * github地址: https://github.com/ChiYaoLa
 *
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class ParamTemplate {

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
        DataSet<Long> dataSource = env.generateSequence(1, 10);

        Configuration config = new Configuration();
        config.setInteger("limit", 6);


        DataSet<Long> result = dataSource.filter(new FilterWithParameters())
                .withParameters(config);
        result.print("输出结果");

        env.execute("ParamTemplate");
    }

    /**
     * github地址: https://github.com/ChiYaoLa
     *
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    private static class FilterWithParameters extends RichFilterFunction<Long> {

        private int limit;

        /**
         * github地址: https://github.com/ChiYaoLa
         *
         * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: xuliang
         * @date: 2020/10/15 18:33
         */
        @Override
        public void open(Configuration parameters) {
            limit = parameters.getInteger("limit", 2);
        }

        /**
         * github地址: https://github.com/ChiYaoLa
         *
         * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: xuliang
         * @date: 2020/10/15 18:33
         */
        @Override
        public boolean filter(Long value) {
            return value > limit;
        }
    }
}