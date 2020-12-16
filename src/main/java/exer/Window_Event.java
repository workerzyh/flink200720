package exer;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author zhengyonghong
 * @create 2020--12--14--9:00
 */
public class Window_Event {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据流
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 9999);

        //设置水位线
        SingleOutputStreamOperator<String> waterMarkDS = input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                return Long.parseLong(split[1])*1000L;
            }
        });

        //转换成sensorreading
        SingleOutputStreamOperator<SensorReading> sensorReading = waterMarkDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //分组、
        KeyedStream<SensorReading, Tuple> keyByDS = sensorReading.keyBy("id");

        //开窗
        WindowedStream<SensorReading, Tuple, TimeWindow> winDS = keyByDS.timeWindow(Time.seconds(30), Time.seconds(5));

        //求最高温度
        SingleOutputStreamOperator<SensorReading> maxTemp = winDS.max("temp");

        //打印测试
        maxTemp.print();

        //启动
        env.execute();
    }
}
