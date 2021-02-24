package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
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

    @Test
    public void testUnion() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> namesrc = env.fromCollection(names);
        DataSource<String> citysrc = env.fromCollection(cities);

//        namesrc.union(citysrc).print("默认union策略");
        namesrc.union(citysrc).union(namesrc).print("验证是否有去重效果：无去重效果");
        env.execute();
    }


    @Test
    public void testCogroup() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<String, Integer>> bupt_boys = new ArrayList<>();
        ArrayList<Tuple2<Integer, String>> bnu_girls = new ArrayList<>();

        bupt_boys.add(new Tuple2<>("xuliang",23));
        bupt_boys.add(new Tuple2<>("xl",25));
        bupt_boys.add(new Tuple2<>("XL",25));

        bnu_girls.add(new Tuple2<>(23,"xuemei1"));
        bnu_girls.add(new Tuple2<>(23,"xuemei2"));
        bnu_girls.add(new Tuple2<>(25,"xuejie"));


        DataSource<Tuple2<String, Integer>> bupt_boy_src = env.fromCollection(bupt_boys);
        DataSource<Tuple2<Integer, String>> bnu_girls_src = env.fromCollection(bnu_girls);

        bupt_boy_src.coGroup(bnu_girls_src).where(1).equalTo(0)
                .with(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<Integer, String>, Tuple3<String,String,Integer>>() {
                    // 我理解 这里的Iterable 其实就是基于前面的where equalto 匹配起来，一对 list pair，而这里的with其实就是如何处理匹配的 list pair
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> boy, Iterable<Tuple2<Integer, String>> girl, Collector<Tuple3<String, String,Integer>> collector) throws Exception {
                        Integer age = 0;

                        Iterator<Tuple2<String, Integer>> boyiter = boy.iterator();
                        String boys_name = "";
                        while (boyiter.hasNext()){
                            Tuple2<String, Integer> next = boyiter.next();
                            boys_name = boys_name+next.f0+" ";
                            age = next.f1;
                        }

                        Iterator<Tuple2<Integer, String>> girl_iter = girl.iterator();
                        String girls_name = "";
                        while (girl_iter.hasNext()){
                            Tuple2<Integer, String> next = girl_iter.next();
                            girls_name = girls_name+next.f1+" ";
                        }

                        collector.collect(new Tuple3<>(boys_name,girls_name,age));


                    }
                }).print("cogroup 男孩女孩匹配结果");
        env.execute();
    }

    
    

}
