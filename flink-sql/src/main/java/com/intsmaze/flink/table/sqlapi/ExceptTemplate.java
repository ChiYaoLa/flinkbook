package com.intsmaze.flink.table.sqlapi;

import com.intsmaze.flink.table.PrepareData;
import com.intsmaze.flink.table.bean.Person;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * github地址: https://github.com/ChiYaoLa
 *
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class ExceptTemplate {


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
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        List<Person> clicksData = PrepareData.getPersonData();
        DataSet<Person> dataStream = env.fromCollection(clicksData);

        tEnv.registerDataSet("Person", dataStream, "name,age,city");

        tEnv.registerDataSet("PersonTmp", dataStream, "name,age,city");

        String except = "SELECT * FROM ( ( SELECT * FROM Person WHERE age<40 ) EXCEPT (SELECT * FROM PersonTmp WHERE age > 33))";

        Table table = tEnv.sqlQuery(except);

        DataSet<Row> result = tEnv.toDataSet(table, Row.class);
        result.print();
    }
}
