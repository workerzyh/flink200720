package window;

import bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhengyonghong
 * @create 2020--12--12--14:29
 */
public class Flink_watermark_trans2 {
    public static void main(String[] args) throws Exception {
        //TODO 滚动时间窗口，计算10s的wordcount
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setParallelism(2);


        //引入事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取端口数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        //转换bean
        SingleOutputStreamOperator<SensorReading> sendorDS = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        SingleOutputStreamOperator<SensorReading> sensorReadingSingleOutputStreamOperator = sendorDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTs() * 1000L;
            }
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorReadingSingleOutputStreamOperator.keyBy("id");

        //开窗
        WindowedStream<SensorReading, Tuple, TimeWindow> sensorReadingTupleTimeWindowWindowedStream = keyedStream.timeWindow(Time.seconds(5));

        //聚合
        SingleOutputStreamOperator<SensorReading> temp = sensorReadingTupleTimeWindowWindowedStream.max("temp");

        //打印
        temp.print();

        //启动
        env.execute();
    }
}
