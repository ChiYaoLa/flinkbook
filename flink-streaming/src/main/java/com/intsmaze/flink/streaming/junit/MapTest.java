package com.intsmaze.flink.streaming.junit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * github地址: https://github.com/ChiYaoLa
 * 
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class MapTest {

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    @Test
    public void testSum() throws Exception {
        MapTemplate mapTemplate = new MapTemplate();

        assertEquals(Long.valueOf(80), mapTemplate.map(40L));
    }
}
