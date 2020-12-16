package transform;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author zhengyonghong
 * @create 2020--12--11--14:31
 */
public class Flink_Transform_connect_coMap {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);

        //转为sensorreading
        SingleOutputStreamOperator<SensorReading> mapStream = stream.map(new Flink_transform_map.MyMapFunc());

        //split，按30度分两支
        SplitStream<SensorReading> split = mapStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });;
        //选择分支
        SingleOutputStreamOperator<Tuple2<String, Double>> high = split.select("high").map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemp());
            }
        });

        DataStream<SensorReading> low = split.select("low");

        //connect连接两条流
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = high.connect(low);

        //合并两条流
        SingleOutputStreamOperator<Object> coMap = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return stringDoubleTuple2;
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return sensorReading;
            }
        });

        //打印测试
        coMap.print();


        //启动
        env.execute();
    }
}
