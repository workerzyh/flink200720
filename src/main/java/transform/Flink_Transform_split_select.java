package transform;

import bean.SensorReading;
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
public class Flink_Transform_split_select {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);
        //转换数据
        SingleOutputStreamOperator<SensorReading> map = stream.map(new Flink_transform_map.MyMapFunc());
        //split分流
        SplitStream<SensorReading> splitDS = map.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        //查询
        DataStream<SensorReading> high = splitDS.select("high");
        DataStream<SensorReading> low = splitDS.select("low");

        //打印测试
        high.print("high");
        low.print("low");

        //启动
        env.execute();

    }
}
