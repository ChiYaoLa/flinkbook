package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Project: flink-book
 * 集合api
 * @author xuliang98
 * File Created at 2021/2/24-4:35 下午
 * @Desc
 */

public class SetApiDemo {

    public List<String> names =  new ArrayList<String>();
    public List<String> cities = new ArrayList<>();

    @Before
    public void datainit(){
        names.add("xuliang");
        names.add("xl");

        cities.add("beijing");
        cities.add("shanghai");
        cities.add("shenzhen");
    }
    
    @Test
    public void testDefaultCross() throws Exception {
        // 不同的cross策略
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> namesrc = env.fromCollection(names);
        DataSource<String> citysrc = env.fromCollection(cities);
//        namesrc.cross(citysrc).print("默认笛卡尔cross结果");
//        namesrc.crossWithHuge(citysrc).print("Hugecross结果");  // 提示citysrc 很大
//        namesrc.crossWithTiny(citysrc).print("tiny cross结果"); // 提示citysrc很小
        namesrc.cross(citysrc).with(new CrossFunction<String, String, Tuple3<String,String,Integer>>() {

            @Override
            public Tuple3<String, String, Integer> cross(String s, String s2) throws Exception {
                int size = s.length() + s2.length();
                return new Tuple3<>(s,s2,size);
            }
        }).print("cross 还可以自定义函数");
        
        env.execute();
    }
    
    

}
