package transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhengyonghong
 * @create 2020--12--11--14:14
 */
public class Flink_transform_keyBy {
    public static void main(String[] args) throws Exception {
        //TODO keyBy测试 通过hash分流
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取端口数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);
        //转换成元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);

            }
        });

        //KEYbY操作
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

        //打印测试
        map.print();
        keyedStream.print();

        //启动
        env.execute();


    }
}
