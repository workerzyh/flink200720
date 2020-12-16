package wordcount;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import wordcount.Flink01_Wordcount_batch;

/**
 * @author zhengyonghong
 * @create 2020--12--09--11:32
 */
public class Flink03_Wordcount_unbounded {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);
        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = stream.flatMap(new Flink01_Wordcount_batch.MyFlatMapFunc());
        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOne.keyBy(0);
        //累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        //打印测试
        result.print();

        //启动任务
        env.execute("Flink03_Wordcount_unbounded");

    }
}
