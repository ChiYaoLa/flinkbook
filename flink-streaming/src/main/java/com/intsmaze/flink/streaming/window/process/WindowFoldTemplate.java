package com.intsmaze.flink.streaming.window.process;


import com.intsmaze.flink.streaming.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * github地址: https://github.com/ChiYaoLa
 * 
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class WindowFoldTemplate {

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));

        DataStream<String> fold = streamSource.keyBy("f0")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .fold("start", new FoldFunction<Tuple3<String, Integer, String>, String>() {
                    @Override
                    public String fold(String accumulator, Tuple3<String, Integer, String> value) {
                        accumulator = accumulator + "--" + value.f1;
                        return accumulator;
                    }
                });

        fold.print("输出结果");

        env.execute("WindowFoldTemplate");
    }
}
