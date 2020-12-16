package source;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author zhengyonghong
 * @create 2020--12--11--10:45
 */
public class Flink_Source_Collection {
    public static void main(String[] args) throws Exception {
        //TODO 从集合读取数据
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据流
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        //打印测试
        sensorReadingDataStreamSource.print();

        //启动
        env.execute();
    }
}
