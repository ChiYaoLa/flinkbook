package com.intsmaze.flink.streaming.window.source;

import com.intsmaze.flink.streaming.window.time.bean.EventBean;
import com.intsmaze.flink.streaming.window.util.TimeUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * github地址: https://github.com/ChiYaoLa
 *
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class SourceWithTimestamps implements SourceFunction<EventBean> {

    public static Logger LOG = LoggerFactory.getLogger(SourceWithTimestamps.class);

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    private int counter = 0;

    private long sleepTime;

    /**
     * github地址: https://github.com/ChiYaoLa
     *
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public SourceWithTimestamps(long sleepTime) {
        this.sleepTime = sleepTime;
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
    public void run(SourceContext<EventBean> ctx) throws Exception {
        while (isRunning) {
            if (counter >= 16) {
                isRunning = false;
            } else {
                EventBean bean = Data.BEANS[counter];
                ctx.collect(bean);
                String time = TimeUtils.getHHmmss(System.currentTimeMillis());
                System.out.println("send 元素内容 : [" + bean + " ] now time:" + time);
                if (bean.getList().get(0).indexOf("nosleep") <= 0) {
                    Thread.sleep(sleepTime);
                }
            }
            counter++;
        }
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
    public void cancel() {
        isRunning = false;
    }


}

