package com.intsmaze.flink.table.sqlapi.groupwindows;

import com.intsmaze.flink.table.util.TimeUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * github地址: https://github.com/ChiYaoLa
 *
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class EventTimeWaterMarks implements AssignerWithPeriodicWatermarks<Row> {

    /**
     * github地址: https://github.com/ChiYaoLa
     *
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {
        Timestamp timestamp = (Timestamp) element.getField(2);
        return timestamp.getTime();
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
    public Watermark getCurrentWatermark() {
        String offset = TimeUtils.getSS(System.currentTimeMillis());
        System.out.println("返回水印------" + offset + "--" + System.currentTimeMillis());
        return new Watermark(System.currentTimeMillis() - 5000);
    }
}