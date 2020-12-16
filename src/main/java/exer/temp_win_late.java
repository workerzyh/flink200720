package exer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * @author zhengyonghong
 * @create 2020--12--15--8:43
 */
public class temp_win_late {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从端口获取数据创建流
        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102", 9999);

        //设置水位线
        SingleOutputStreamOperator<String> watermarkDS = inputDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        //map转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = watermarkDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], 1);
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> KeyedStream = mapDS.keyBy(0);

        //开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> winDS = KeyedStream.timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sensor_temp") {
                });

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = winDS.sum(1);

        //主流打印测试
        result.print("主流");

        //侧输出流打印测试
        DataStream<Tuple2<String, Integer>> sideOutput = result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sensor_temp") {
        });
        sideOutput.print("侧输出流");

        //启动
        env.execute();

    }
}
