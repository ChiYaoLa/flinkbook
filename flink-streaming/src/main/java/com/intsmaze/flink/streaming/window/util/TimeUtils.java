package com.intsmaze.flink.streaming.window.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * github地址: https://github.com/ChiYaoLa
 * 
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class TimeUtils {

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public static String getHHmmss(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss SSS");
        String str = sdf.format(date);
        return "时间:" + str;
    }

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public static String getHHmmss(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        String str = sdf.format(new Date(time));
        return "时间:" + str;
    }

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public static String getSs(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("ss SSS");
        String str = sdf.format(date);
        return "时间:" + str;
    }

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public static String getSs(Long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("ss SSS");
        String str = sdf.format(new Date(time));
        return str;
    }

}
