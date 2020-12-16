package transform;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * @author zhengyonghong
 * @create 2020--12--11--14:31
 */
public class Flink_Transform_union {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> map = stream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        //分支
        SplitStream<SensorReading> split = map.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        //选择
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");

        //union
        DataStream<SensorReading> union = high.union(low);
        union.print();

        env.execute();

    }
}
