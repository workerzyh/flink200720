package window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author zhengyonghong
 * @create 2020--12--14--10:53
 */
public class Flink_WaterMark_sessionWin {
    public static void main(String[] args) throws Exception {
        //TODO 会话窗口用水位线
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        //设置水位线
        SingleOutputStreamOperator<String> waterMarkDS = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = waterMarkDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], 1);
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = map.keyBy(0);

        //会话开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = tuple2TupleKeyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);

        //打印测试
        sum.print();

        //启动
        env.execute();

    }
}
