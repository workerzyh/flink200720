package window;

import bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.swing.*;
import java.sql.ResultSet;

/**
 * @author zhengyonghong
 * @create 2020--12--14--9:00
 */
public class Flink_Window_late {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //引入事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取端口数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        //TODO 水位线引入,指定时间字段
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            //抽取时间戳
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

        //TODO 开窗 ，允许迟到2s，且超过2s后窗口关闭进入侧输出流
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> winDS = keyByStream.timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("slide_out") {
                });

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = winDS.sum(1);

        //主流打印
        result.print("主流");

        //侧输出流打印
        DataStream<Tuple2<String, Integer>> sideOutput = result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("slide_out") {
        });
        sideOutput.print("侧输出流");
        //启动
        env.execute();
    }
}
