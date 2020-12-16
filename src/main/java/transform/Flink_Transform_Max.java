package transform;

import bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhengyonghong
 * @create 2020--12--11--14:31
 */
public class Flink_Transform_Max {
    public static void main(String[] args) throws Exception {
        //TODO 最大温度
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 9999);
        //转化成sensorreading
        SingleOutputStreamOperator<SensorReading> map = stream.map(new Flink_transform_map.MyMapFunc());

        //分组
        KeyedStream<SensorReading, Tuple> id = map.keyBy("id");
        //求最高温度
        SingleOutputStreamOperator<SensorReading> temp = id.max("temp");

        //打印测试
        temp.print();

        env.execute();

    }
}
