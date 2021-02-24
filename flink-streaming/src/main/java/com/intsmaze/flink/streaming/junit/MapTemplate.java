package com.intsmaze.flink.streaming.junit;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * github地址: https://github.com/ChiYaoLa
 * 
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class MapTemplate implements MapFunction<Long, Long> {

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    @Override
    public Long map(Long value1) throws Exception {
        return value1 * 2;
    }
}
