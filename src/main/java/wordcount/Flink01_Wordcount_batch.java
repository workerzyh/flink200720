package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author zhengyonghong
 * @create 2020--12--09--10:47
 */
public class Flink01_Wordcount_batch {
    public static void main(String[] args) throws Exception {
        //TODO 批处理wordcount
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataSource<String> input = env.readTextFile("input");
        //压平
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = input.flatMap(new MyFlatMapFunc());
        //分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);
        //累加
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);
        //打印测试
        result.print();
    }
    public static class MyFlatMapFunc implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
