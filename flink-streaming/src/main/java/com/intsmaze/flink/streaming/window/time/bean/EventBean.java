package com.intsmaze.flink.streaming.window.time.bean;

import com.intsmaze.flink.streaming.window.util.TimeUtils;

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
public class EventBean {

    private List<String> list;

    private long time;

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public EventBean(String text, long time) {
        list = new ArrayList<String>();
        list.add(text);
        this.time = time;
    }

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public EventBean() {
    }

    public long getTime() {
        return time;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

    public void setTime(long time) {
        this.time = time;
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
    public String toString() {
        return "{" +
                "text='" + list.toString() + '\'' +
                ", time=" + TimeUtils.getHHmmss(time) +
                '}';
    }
}
