package window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhengyonghong
 * @create 2020--12--12--14:29
 */
public class Flink_watermark_trans1 {
    public static void main(String[] args) throws Exception {
        //TODO 滚动时间窗口，计算10s的wordcount
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setParallelism(2);


        //引入事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取端口数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        //TODO 水位线引入,指定时间字段
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = stringSingleOutputStreamOperator.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new Tuple2<>(split[0], 1));
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByStream = wordToOne.keyBy(0);

        //开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> winDS = keyByStream.timeWindow(Time.seconds(5));

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = winDS.sum(1);

        //打印
        result.print();

        //启动
        env.execute();
    }
}
