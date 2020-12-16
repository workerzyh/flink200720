package taskchain;

import wordcount.Flink01_Wordcount_batch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhengyonghong
 * @create 2020--12--11--10:24
 */
public class Flink2_WordCount_parallelism {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(2);
        //读取端口数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new Flink01_Wordcount_batch.MyFlatMapFunc());
        //过滤，不要hello
        SingleOutputStreamOperator<Tuple2<String, Integer>> filter = wordToOne.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return !"hello".equals(value.f0);
            }
        });
        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> kebyDStream = filter.keyBy(0);
        //累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = kebyDStream.sum(1);
        //打印测试
        result.print();
        //启动
        env.execute("测试");

    }

}
