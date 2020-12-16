package exer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import transform.Flink_transform_map;
import wordcount.Flink01_Wordcount_batch;

/**
 * @author zhengyonghong
 * @create 2020--12--13--14:29
 */
public class notSum_wordcount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new Flink01_Wordcount_batch.MyFlatMapFunc());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDS = wordToOne.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyByDS.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });
        //打印测试
        reduce.print();

        //启动
        env.execute();
    }
}
