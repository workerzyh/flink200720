package transform;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhengyonghong
 * @create 2020--12--11--14:03
 */
public class Flink_transform_map {
    public static void main(String[] args) throws Exception {
        //TODO map
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据流
        DataStreamSource<String> input = env.readTextFile("input/sensorreading.txt");

        //map转换
        SingleOutputStreamOperator<SensorReading> map = input.map(new MyMapFunc());

        //打印测试
        map.print();

        //启动
        env.execute();
    }

    public static class MyMapFunc implements MapFunction<String,SensorReading>{

        @Override
        public SensorReading map(String value) throws Exception {
            String[] words = value.split(",");
            return new SensorReading(words[0],Long.parseLong(words[1]),Double.parseDouble(words[2]));
        }
    }
}
