package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
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
 * 集合api    https://ci.apache.org/projects/flink/flink-docs-stable/dev/batch/dataset_transformations.html#groupcombine-on-a-grouped-dataset
 * 
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


    // cross 是交叉链接，笛卡尔积
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

    // join 就是 inner join ，还有其他left join ，right join这些，是肯定要on 什么条件才发生的连接匹配，跟cross不同
    @Test
    public void testJoin() throws Exception {
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
        
        //最简单的inner join
//        bupt_boy_src.join(bnu_girls_src).where(1).equalTo(0)
//                .with(new JoinFunction<Tuple2<String, Integer>, Tuple2<Integer, String>, Tuple2<String,String>>() {
//                    @Override
//                    public Tuple2<String, String> join(Tuple2<String, Integer> boy, Tuple2<Integer, String> girl) throws Exception {
//
//                        return new Tuple2<>(boy.f0,girl.f1);
//                    }
//                }).print("简单的inner join");
        // 复杂的flat join
        bupt_boy_src.join(bnu_girls_src).where(1).equalTo(0)
                .with(new FlatJoinFunction<Tuple2<String, Integer>, Tuple2<Integer, String>, Tuple2<String,String>>() {
                    @Override
                    public void join(Tuple2<String, Integer> boys, Tuple2<Integer, String> girls, Collector<Tuple2<String, String>> collector) throws Exception {
                        // 当输入输出不是一对一，出现了 1 对0 ，1对n，就要用flat操作，比如下面这个 有一定概率一对0 一堆n
                        if (boys.f0.startsWith("xu")){
                            collector.collect(new Tuple2<>(boys.f0,girls.f1));
                        }
                    }
                }).print("flat类api操作，可以实现数据概率过滤");
        
        env.execute();

    }

    @Test
    public void testConnected(){
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> citysrc = env.fromCollection(cities);


        List<Tuple3<String, Integer, Double>> list = new ArrayList<>();
        list.add(new Tuple3<>("张三", 15, 999.9));
        list.add(new Tuple3<>("张三", 30, 1899.0));
        list.add(new Tuple3<>("张三", 21, 3000.89));
        list.add(new Tuple3<>("李四", 31, 188.88));
        list.add(new Tuple3<>("王五", 55, 99.99));
        list.add(new Tuple3<>("王五", 67, 18.88));

        DataSource<Tuple3<String, Integer, Double>> namesrc = env.fromCollection(list);
        namesrc.groupBy(0).aggregate(Aggregations.MIN, 1);


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


    // cogroup 有inner的感觉，但是比inner join的定制化结果能力更强
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

    // 各种针对 group by 的后操作
    @Test
    public void testGroupReduce() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<String, Integer>> bupt_boys = new ArrayList<>();
        ArrayList<Tuple2<Integer, String>> bnu_girls = new ArrayList<>();

        bupt_boys.add(new Tuple2<>("xuliang",23));
        bupt_boys.add(new Tuple2<>("xl",25));
        bupt_boys.add(new Tuple2<>("liangge",25));

        DataSource<Tuple2<String, Integer>> src = env.fromCollection(bupt_boys);

        //reduce group
        src.groupBy(1).reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {

                String name = "";
                Integer age = 0;

                Iterator<Tuple2<String, Integer>> iter = iterable.iterator();
                while (iter.hasNext()){
                    Tuple2<String, Integer> next = iter.next();
                    name = name + next.f0 + "#";
                    age = next.f1;
                }

                collector.collect(new Tuple2<>(name,age));

            }
        }).print("reduce group结果");


        // sort group 也可以与reduce-group连着用
        src.groupBy(1).sortGroup(0, Order.ASCENDING).first(1).print("sort group简单尝试"); // 相当于把每个组的第一个元素取出来
        env.execute();

    }


    public void testProjection(){
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple3<String, Integer,String>> bupt_boys = new ArrayList<>();

        bupt_boys.add(new Tuple3<>("xuliang",23,"@@"));
        bupt_boys.add(new Tuple3<>("xl",25,"##"));
        bupt_boys.add(new Tuple3<>("liangge",25,"#¥#¥#"));

        DataSource<Tuple3<String, Integer, String>> src = env.fromCollection(bupt_boys);

    }
    
    

}
