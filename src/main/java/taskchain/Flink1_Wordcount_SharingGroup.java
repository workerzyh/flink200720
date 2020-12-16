package taskchain;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhengyonghong
 * @create 2020--12--09--11:32
 */
public class Flink1_Wordcount_SharingGroup {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据流
        DataStreamSource<String> input = env.readTextFile("input/1.txt");
        //压平数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }

            }
        }).slotSharingGroup("group1");
        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);
        //累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1).slotSharingGroup("group2");
        //打印测试
        result.print().slotSharingGroup("group3");
        //启动
        env.execute();
    }
}
