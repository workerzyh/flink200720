package checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhengyonghong
 * @create 2020--12--15--20:48
 */
public class Flink_CheckPoint_Test {
    public static void main(String[] args) throws Exception {
        // 设置访问HDFS集群的用户名
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoints"));

        //开启checkpoint
        env.enableCheckpointing(10000L);

        //从端口获取数据并创建流
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> KeyedStream = wordToOne.keyBy(0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = KeyedStream.sum(1);

        //打印测试
        result.print();

        //启动
        env.execute();

    }
}
