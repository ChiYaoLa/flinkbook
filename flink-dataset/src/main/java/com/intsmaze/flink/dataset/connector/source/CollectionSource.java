package com.intsmaze.flink.dataset.connector.source;

import com.intsmaze.flink.dataset.bean.Trade;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;

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
public class CollectionSource {


    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取本地文件
        String filePath = "file:////Users/xuliang98/Documents/java/iot/flink-book/flink-dataset/src/main/resources/testnumfile";
        TextInputFormat format = new TextInputFormat(new Path(filePath));
        BasicTypeInfo<Integer> typeInfo = BasicTypeInfo.INT_TYPE_INFO;
        DataSet<String> textInputFormat = env.readFile(format, filePath);
        textInputFormat.print("本地文件 按照一定的格式");


        DataSet<String> localLines = env.readTextFile("file:////Users/xuliang98/Documents/java/iot/flink-book/flink-dataset/src/main/resources/testtextfile");
        localLines.print("文本格式读文件");

        // 读取hdfs文件
//        DataSet<String> hdfsLines = env.readTextFile("hdfs://name-node-Host:Port/intsmaze/to/textfile");
//        DataSet<Tuple2<String, Double>> csvInput = env.readCsvFile("hdfs:///intsmaze/CSV/file")
//                .includeFields("10010")
//                .includeFields(true, false, false, true, false)
//                .types(String.class, Double.class);


//        DataSet<Trade> csvInput1 = env.readCsvFile("hdfs:///intsmaze/CSV/file")
//                .pojoType(Trade.class, "name", "age", "city");


        // 从元素或者集合创建
        DataSet<String> value = env.fromElements("flink", "strom", "spark", "stream");

        List<String> list = new ArrayList<>();
        list.add("HashMap");
        list.add("List");
        env.fromCollection(list).print();

        env.fromElements("Flink", "intsmaze").print();
        env.generateSequence(100, 106).print();
    }
}
